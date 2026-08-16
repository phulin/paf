from __future__ import annotations

import asyncio
import hashlib
import re
from collections import deque
from collections.abc import Coroutine, Iterable
from contextlib import suppress
from dataclasses import dataclass, replace
from typing import Any
from uuid import uuid4

from paf.codex import (
    DOWNSTREAM_RETRY_ROLE,
    UPSTREAM_REPAIR_ROLE,
    AgentResult,
    CodexExecutor,
    ValidationResult,
    count_placeholders,
    declaration_uses_placeholder,
    declaration_uses_placeholder_in_chapter,
    scope_digest,
    scoped_files,
    validate,
)
from paf.coordination import CoordinatorBuildQueue, PriorityLimiter
from paf.corpus import (
    WorkUnitImportGraph,
    build_corpus_schedule,
    build_source_dependency_graph,
    scheduling_snapshot,
)
from paf.diagnostics import unexpected_lean_warnings
from paf.git import GitCommitter
from paf.isolation import IsolationResult, create_isolation
from paf.models import PipelineConfig, Stage, WorkUnit, WorkUnitLike
from paf.scope import ScopeMatcher
from paf.state import (
    RunRecord,
    StateStore,
    TaskStatus,
    UpstreamRequestStatus,
)


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

    def feedback(self) -> str:
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
        if not self.validation.succeeded:
            parts.append("Validation failed:\n" + self.validation.output)
        return "\n\n".join(parts)


@dataclass(frozen=True)
class FormalizeOutcome:
    succeeded: bool


@dataclass(frozen=True)
class ReviewOutcome:
    succeeded: bool
    changed: bool
    complete: bool = True
    run_id: str = ""


@dataclass(frozen=True)
class BuildDiagnostics:
    actionable: dict[str, str]
    deferred_owner_ids: tuple[str, ...]


@dataclass(frozen=True)
class ValidatedBuildSnapshot:
    graph: WorkUnitImportGraph
    source_digests: dict[str, str]


@dataclass(frozen=True)
class PendingBuildRequest:
    chapters: tuple[WorkUnitLike, ...]
    publish_if_clean: bool
    mode: str
    iteration: int
    maximum_iterations: int
    stage: Stage
    priority: float
    preemptible: bool
    snapshots: dict[str, ValidatedBuildSnapshot] | None
    future: asyncio.Future[dict[str, ValidationResult]]


@dataclass(frozen=True)
class RunningFormalizeStage:
    task: asyncio.Task[bool]
    progress: asyncio.Event
    target_ids: frozenset[str]


@dataclass(frozen=True)
class RunningReview:
    task: asyncio.Task[bool]
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
PROOF_FEEDBACK_MAX_CHARS = 24_000
PROOF_FEEDBACK_ROUNDS = 3


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
        self.build_queue = CoordinatorBuildQueue()
        self._pending_build_requests: list[PendingBuildRequest] = []
        self._build_dispatch_task: asyncio.Task[None] | None = None
        self._build_batch_tasks: set[asyncio.Task[None]] = set()
        # Snapshot creation and scoped source integration need a short
        # consistency barrier with main-worktree builds. Unlike build_queue,
        # this lock is never held for an overlay agent's editing lifetime or
        # for a proof validation outside a coordinator build.
        self.source_lock = asyncio.Lock()
        self._formalize_graph_lock = asyncio.Lock()
        self._discovery_lock = asyncio.Lock()
        self._invalidated_reviews: set[str] = set()
        self._proof_rechecks: set[str] = set()
        self._review_invalidation_generations: dict[str, int] = {}
        self._review_generation_lock = asyncio.Lock()
        self._chapter_agent_locks = {chapter.id: asyncio.Lock() for chapter in self.work_units}
        self._upstream_repair_tasks: dict[str, asyncio.Task[None]] = {}

    @property
    def chapters(self) -> tuple[WorkUnitLike, ...]:
        """Compatibility view for callers using the previous domain name."""

        return self.work_units

    def scheduling_snapshot(self) -> dict[str, object]:
        return scheduling_snapshot(self.statement_schedule, self.proof_schedule)

    async def prepare(self) -> None:
        await self.state.load_or_create()
        await self.state.requeue_interrupted(resume_agents=self.resume_agents)
        self.scaffold()
        await self._recover_upstream_requests()
        migrated = await self.state.migrate_post_review_fixups()
        if migrated:
            # The normal review scheduler reports an invalid import graph.
            with suppress(ValueError):
                await self._invalidate_reviews(
                    migrated,
                    detail="recovered post-review findings",
                )
        await self.executor.prepare()
        await self.isolation.prepare()
        await self.git.prepare()

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

    async def shutdown(self) -> None:
        try:
            repairs = tuple(self._upstream_repair_tasks.values())
            for task in repairs:
                task.cancel()
            await asyncio.gather(*repairs, return_exceptions=True)
            self._upstream_repair_tasks.clear()
            if self._build_dispatch_task is not None:
                self._build_dispatch_task.cancel()
                await asyncio.gather(self._build_dispatch_task, return_exceptions=True)
                self._build_dispatch_task = None
            batches = tuple(self._build_batch_tasks)
            for task in batches:
                task.cancel()
            await asyncio.gather(*batches, return_exceptions=True)
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

    def _observed_work_unit_graph(self) -> WorkUnitImportGraph:
        nodes = self.state.source_dependency_tree.get("nodes", {})
        return build_source_dependency_graph(
            self.work_units,
            nodes if isinstance(nodes, dict) else {},
        )

    def _source_input_digest(self, chapter: WorkUnitLike) -> str:
        path = self.config.settings.repo / chapter.source
        lines = path.read_text(encoding="utf-8").splitlines()
        selected = "\n".join(
            lines[chapter.source_span.start_line - 1 : chapter.source_span.end_line]
        )
        return hashlib.sha256(f"{chapter.id}\0{selected}".encode()).hexdigest()

    def _discovery_is_current(self, chapter: WorkUnitLike) -> bool:
        nodes = self.state.source_dependency_tree.get("nodes", {})
        record = nodes.get(chapter.id, {}) if isinstance(nodes, dict) else {}
        return (
            self.state.task(chapter.id, Stage.DISCOVER).status == TaskStatus.SUCCEEDED
            and isinstance(record, dict)
            and record.get("source_digest") == self._source_input_digest(chapter)
        )

    async def _persist_source_dependencies(
        self,
        chapter: WorkUnitLike,
        dependencies: Iterable[str],
        report: dict[str, Any],
    ) -> None:
        async with self._discovery_lock:
            previous = self.state.source_dependency_tree
            raw_nodes = previous.get("nodes", {}) if isinstance(previous, dict) else {}
            nodes = dict(raw_nodes) if isinstance(raw_nodes, dict) else {}
            nodes[chapter.id] = {
                "dependencies": sorted(set(dependencies)),
                "source_digest": self._source_input_digest(chapter),
                "summary": str(report.get("summary", "")),
                "issues": list(report.get("issues", ())),
            }
            graph = build_source_dependency_graph(self.work_units, nodes)
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
            await self.state.save()

    def _retain_formalize_clean(
        self,
        graph: WorkUnitImportGraph,
        records: dict[str, Any],
    ) -> dict[str, dict[str, Any]]:
        """Retain build records whose own source is unchanged."""

        by_id = {chapter.id: chapter for chapter in self.work_units}
        retained: dict[str, dict[str, Any]] = {}
        for chapter_id in graph.order:
            if not graph.dependencies[chapter_id].issubset(retained):
                continue
            record = records.get(chapter_id)
            if not isinstance(record, dict):
                continue
            source = scope_digest(self.config.settings.repo, by_id[chapter_id])
            if record.get("source_digest") == source:
                retained[chapter_id] = {
                    "source_digest": source,
                    "build_generation": int(record.get("build_generation", 0)),
                }
        return retained

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
            required = self._dependency_closure(graph, snapshots)
            for snapshot in snapshots.values():
                if snapshot.graph.edges != graph.edges:
                    return False
                for chapter_id, digest in snapshot.source_digests.items():
                    existing = captured.setdefault(chapter_id, digest)
                    if existing != digest:
                        return False
            if not required.issubset(captured):
                return False

            by_id = {item.id: item for item in self.work_units}
            current = {
                chapter_id: scope_digest(self.config.settings.repo, by_id[chapter_id])
                for chapter_id in required
            }
            if any(captured[chapter_id] != digest for chapter_id, digest in current.items()):
                return False

            persisted = self.state.formalize_graph.get("clean", {})
            records = persisted if isinstance(persisted, dict) else {}
            clean = self._retain_formalize_clean(graph, records)
            build_generation = int(self.state.formalize_graph.get("build_generation", 0))
            for chapter_id in graph.order:
                if chapter_id not in required:
                    continue
                source = captured[chapter_id]
                record = clean.get(chapter_id)
                if isinstance(record, dict) and record.get("source_digest") == source:
                    continue
                build_generation += 1
                clean[chapter_id] = {
                    "source_digest": source,
                    "build_generation": build_generation,
                }
            await self._save_formalize_graph(
                graph,
                clean,
                build_generation=build_generation,
                validated=required,
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
        """Mark an edited source closure stale before any verification is queued."""

        graph = self._observed_work_unit_graph()
        persisted = self.state.formalize_graph.get("clean", {})
        clean = self._retain_formalize_clean(
            graph,
            persisted if isinstance(persisted, dict) else {},
        )
        invalidated = self._invalidate_formalize_descendants(graph, clean, chapter_ids)
        await self._save_formalize_graph(
            graph,
            clean,
            build_generation=int(self.state.formalize_graph.get("build_generation", 0)),
            invalidated=invalidated,
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
    ) -> int:
        async with self._formalize_graph_lock:
            explicitly_invalidated = set(invalidated)
            explicitly_validated = set(validated)
            previous = self.state.formalize_graph
            previous_edges = previous.get("edges") if isinstance(previous, dict) else None
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
            retained = self._retain_formalize_clean(graph, clean)
            clean.clear()
            clean.update(retained)
            edges = [list(edge) for edge in graph.edges]
            revision = int(previous.get("revision", 0)) if isinstance(previous, dict) else 0
            if previous.get("algorithm") != "source-dependency-tree" or previous_edges != edges:
                revision += 1
            build_generation = max(
                build_generation,
                int(previous.get("build_generation", 0)) if isinstance(previous, dict) else 0,
            )
            dirty = set(previous.get("dirty", ())) if isinstance(previous, dict) else set()
            dirty.update(explicitly_invalidated)
            dirty.difference_update(explicitly_validated)
            self.state.formalize_graph = graph.snapshot() | {
                "algorithm": "source-dependency-tree",
                "revision": revision,
                "build_generation": build_generation,
                "clean": clean,
                "dirty": sorted(dirty),
            }
            await self.state.save()
            return revision

    def _scope_exists(self, chapter: WorkUnitLike) -> bool:
        return ScopeMatcher(chapter.scope).has_match_for_each_pattern(self.config.settings.repo)

    def _proof_build_is_fresh(self, chapter: WorkUnitLike) -> bool:
        """Whether the current chapter source belongs to a retained clean build."""

        graph = self._observed_work_unit_graph()
        persisted = self.state.formalize_graph.get("clean", {})
        clean = self._retain_formalize_clean(
            graph,
            persisted if isinstance(persisted, dict) else {},
        )
        return chapter.id in clean

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
        if isolated.accepted and isolated.changed_paths and stage is not Stage.PROVE:
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
    ) -> Attempt:
        auxiliary = role == UPSTREAM_REPAIR_ROLE
        selected_request_ids = tuple(dict.fromkeys(request_ids))
        selected_upstream_requests = tuple(upstream_requests)
        await self.control.checkpoint()
        schedule = (
            self.statement_schedule
            if stage in (Stage.FORMALIZE, Stage.FORMALIZE, Stage.REVIEW)
            else self.proof_schedule
        )
        chapter_lock = self._chapter_agent_locks[chapter.id]
        await chapter_lock.acquire()
        chapter_lock_held = True
        slot_held = False
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
            await self.agent_slots.acquire(schedule.priority(chapter.document_id))
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
        try:
            if auxiliary:
                run = await self.state.start_auxiliary_run(
                    chapter.id,
                    stage,
                    role=role,
                    request_ids=selected_request_ids,
                )
            else:
                run = await self.state.start_run(chapter.id, stage)
                if role:
                    await self.state.update_run(
                        run,
                        role=role,
                        request_ids=list(selected_request_ids),
                    )
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
            if auxiliary:
                agent = await self.executor.run_upstream_repair(
                    chapter,
                    run,
                    selected_upstream_requests,
                    workspace_root=workspace_root,
                )
            else:
                agent = await self.executor.run(
                    chapter,
                    stage,
                    run,
                    feedback=feedback,
                    workspace_root=workspace_root,
                )
            self.agent_slots.release()
            slot_held = False
            # Agent capacity covers live Codex processes, not integration or a
            # potentially preempted coordinator build queued after they exit.
            if live_discovery:
                isolated = IsolationResult(accepted=True, generation=0)
            else:
                if not source_held:
                    await self.source_lock.acquire()
                    source_held = True
                assert workspace is not None
                isolated = await workspace.collect(chapter, integration_lock=None)
                isolated = await self._commit_agent_changes(chapter, stage, agent, isolated)
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
                )
            ):
                invalidated_builds = await self._invalidate_build_records((chapter.id,))
                if stage is Stage.REVIEW or auxiliary:
                    self._proof_rechecks.update(invalidated_builds)
            if isolated.accepted:
                if stage is Stage.PROVE and (agent.changed or self.force):
                    snapshots: dict[str, ValidatedBuildSnapshot] = {}
                    validation = (
                        await self._build_chapters(
                            (chapter,),
                            publish_if_clean=True,
                            mode=(
                                "upstream-repair-certification"
                                if auxiliary
                                else "proof-certification"
                            ),
                            stage=Stage.PROVE,
                            priority=250.0 if auxiliary else 0.0,
                            preemptible=not auxiliary,
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
                        )
                elif stage is Stage.PROVE:
                    build_fresh = self._proof_build_is_fresh(chapter)
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
                self.agent_slots.release()
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

    async def _discover(self, chapter: WorkUnitLike, *, rerun: bool = False) -> FormalizeOutcome:
        if not rerun and not self.force and self._discovery_is_current(chapter):
            return FormalizeOutcome(True)
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
            return FormalizeOutcome(False)
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
                return FormalizeOutcome(False)
            await self.state.set_task(
                chapter.id,
                Stage.DISCOVER,
                TaskStatus.SUCCEEDED,
                "source dependency tree persisted",
            )
            return FormalizeOutcome(True)
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
        return FormalizeOutcome(False)

    async def _formalize(self, chapter: WorkUnitLike, *, rerun: bool = False) -> FormalizeOutcome:
        if self._already_done(chapter, Stage.FORMALIZE):
            return FormalizeOutcome(True)
        maximum = self.config.stages[Stage.FORMALIZE].max_rounds
        feedback = ""
        for iteration in range(1, maximum + 1):
            if self._scope_exists(chapter):
                snapshots: dict[str, ValidatedBuildSnapshot] = {}
                validation = (
                    await self._build_chapters(
                        (chapter,),
                        publish_if_clean=True,
                        mode="source-tree-formalize",
                        iteration=iteration,
                        maximum_iterations=maximum,
                        stage=Stage.FORMALIZE,
                        priority=100.0,
                        snapshots=snapshots,
                    )
                )[chapter.id]
                if validation.succeeded and await self._publish_validated_build(
                    chapter, snapshots[chapter.id]
                ):
                    await self.state.set_task(
                        chapter.id,
                        Stage.FORMALIZE,
                        TaskStatus.SUCCEEDED,
                        "clean diagnostics and coordinator build in source dependency order",
                    )
                    return FormalizeOutcome(True)
                feedback = self._build_feedback({chapter.id: validation}).actionable.get(
                    chapter.id, validation.output
                )

            attempt = await self._attempt(
                chapter,
                Stage.FORMALIZE,
                feedback=feedback,
                queue_detail=f"dependency-ready formalization pass {iteration}/{maximum}",
            )
            complete = bool(attempt.agent.report.get("complete"))
            if attempt.agent.capacity_exhausted:
                await self.state.set_task(
                    chapter.id,
                    Stage.FORMALIZE,
                    TaskStatus.FAILED,
                    "model capacity remained unavailable after the configured retries",
                )
                return FormalizeOutcome(False)
            if not attempt.agent.succeeded or not attempt.validation.succeeded or not complete:
                await self.state.set_task(
                    chapter.id,
                    Stage.FORMALIZE,
                    TaskStatus.FAILED,
                    "formalizer failed or reported incomplete coverage and diagnostics",
                )
                return FormalizeOutcome(False)

        if self._scope_exists(chapter):
            snapshots = {}
            validation = (
                await self._build_chapters(
                    (chapter,),
                    publish_if_clean=True,
                    mode="source-tree-formalize-final",
                    iteration=maximum,
                    maximum_iterations=maximum,
                    stage=Stage.FORMALIZE,
                    priority=100.0,
                    snapshots=snapshots,
                )
            )[chapter.id]
            if validation.succeeded and await self._publish_validated_build(
                chapter, snapshots[chapter.id]
            ):
                await self.state.set_task(
                    chapter.id,
                    Stage.FORMALIZE,
                    TaskStatus.SUCCEEDED,
                    "clean diagnostics and coordinator build in source dependency order",
                )
                return FormalizeOutcome(True)

        await self.state.set_task(
            chapter.id,
            Stage.FORMALIZE,
            TaskStatus.FAILED,
            f"formalization did not reach clean diagnostics in {maximum} attempts",
        )
        return FormalizeOutcome(False)

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

    def _path_owner_ids(self, path: str) -> tuple[str, ...]:
        normalized = path.replace("\\", "/")
        repo_prefix = self.config.settings.repo.as_posix().rstrip("/") + "/"
        normalized = normalized.removeprefix(repo_prefix).removeprefix("./")
        owners: list[str] = []
        for chapter in self.work_units:
            root = (chapter.lean_root / chapter.chapter_path).as_posix()
            lean_prefix = self.config.settings.lean_project.as_posix().rstrip("/") + "/"
            roots = (root, root.removeprefix(lean_prefix))
            if any(
                normalized == f"{item}.lean" or normalized.startswith(f"{item}/") for item in roots
            ):
                owners.append(chapter.id)
        return tuple(dict.fromkeys(owners))

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
        """Resolve one proposed owner and retain a reason for every rejected handoff."""

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
        if proposed_owner != owner_id:
            return (
                request,
                owner_id,
                (f"proposed owner {proposed_owner!r} disagrees with path owner {owner_id!r}"),
            )
        by_id = {chapter.id: chapter for chapter in self.work_units}
        owner = by_id.get(owner_id)
        if owner is None:
            return request, owner_id, "proposed owner chapter is outside this swarm selection"
        if not self._is_earlier_work_unit(owner, consumer):
            return (
                request,
                owner_id,
                (f"proposed owner {owner_id} is not chronologically earlier than {consumer.id}"),
            )
        request.update({name: str(request[name]).strip() for name in required_strings})
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
            if not self._proof_build_is_fresh(owner):
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
            if (
                attempt.agent.changed
                and owner_proof.status == TaskStatus.SUCCEEDED
                and count_placeholders(self.config.settings.repo, owner) == 0
            ):
                await self.state.set_task(
                    owner.id,
                    Stage.PROVE,
                    TaskStatus.SUCCEEDED,
                    "proved upstream interface repair validated",
                    source_digest=scope_digest(self.config.settings.repo, owner),
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

    def _proof_review_feedback(
        self,
        chapter_id: str,
    ) -> tuple[str, tuple[str, ...]]:
        blocks: dict[str, None] = {}
        request_ids: list[str] = []
        for request_id, value in self.state.proof_review_requests.items():
            feedback = value.get("feedback") if isinstance(value, dict) else None
            block = feedback.get(chapter_id) if isinstance(feedback, dict) else None
            if not isinstance(block, str) or not block.strip():
                continue
            request_ids.append(request_id)
            blocks[block] = None
        return "\n\n".join(blocks), tuple(request_ids)

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
        """Recover the proof-to-review handoff if a process died between its durable steps."""

        persisted_origins = {
            value.get("origin_run_id")
            for value in self.state.proof_review_requests.values()
            if isinstance(value, dict)
        }
        for chapter in self.work_units:
            proof_runs = [
                run for run in self.state.task(chapter.id, Stage.PROVE).runs if not run.auxiliary
            ]
            if not proof_runs:
                continue
            run = proof_runs[-1]
            self.state.load_run_details(run)
            report = run.report if isinstance(run.report, dict) else {}
            if not report.get("failed_attempts") or run.id in persisted_origins:
                continue
            review_runs = self.state.task(chapter.id, Stage.REVIEW).runs
            reviewed_after = any(
                review_run.status == TaskStatus.SUCCEEDED
                and review_run.finished_at is not None
                and review_run.finished_at >= (run.finished_at or run.started_at)
                for review_run in review_runs
            )
            if reviewed_after:
                continue
            await self._queue_proof_review(chapter, report, origin_run_id=run.id)
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
        return tuple(
            chapter.id
            for chapter in self.work_units
            if module == chapter.chapter_module or module.startswith(chapter.chapter_module + ".")
        )

    def _diagnostic_owner_ids(self, diagnostic: LeanDiagnostic) -> tuple[str, ...]:
        message = diagnostic.header.split(":", 1)[1].lstrip()
        if location := LEAN_LOCATION_RE.match(message):
            return self._path_owner_ids(location.group("path"))

        # Some orchestration failures have no line/column but do name one or
        # more modules or source roots. Matching only this diagnostic block is
        # intentional: matching the complete Lake transcript over-assigns every
        # replayed dependency and permitted `sorry` warning.
        owners: list[str] = []
        for chapter in self.work_units:
            for identifier in self._chapter_identifiers(chapter):
                pattern = rf"(?<![A-Za-z0-9_]){re.escape(identifier)}(?![A-Za-z0-9_])"
                if re.search(pattern, diagnostic.text):
                    owners.append(chapter.id)
                    break
        return tuple(dict.fromkeys(owners))

    async def _build_chapters(
        self,
        chapters: Iterable[WorkUnitLike],
        *,
        publish_if_clean: bool,
        mode: str = "targeted",
        iteration: int = 1,
        maximum_iterations: int = 1,
        stage: Stage = Stage.FORMALIZE,
        priority: float = 100.0,
        preemptible: bool = False,
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
            priority=priority,
            preemptible=preemptible,
            snapshots=snapshots,
            future=future,
        )
        self._pending_build_requests.append(request)
        if self._build_dispatch_task is None or self._build_dispatch_task.done():
            self._build_dispatch_task = asyncio.create_task(self._dispatch_build_requests())
        return await future

    async def _dispatch_build_requests(self) -> None:
        """Let concurrent callers enqueue, then launch one shared build transaction."""

        await asyncio.sleep(0)
        requests = tuple(
            request for request in self._pending_build_requests if not request.future.cancelled()
        )
        self._pending_build_requests.clear()
        self._build_dispatch_task = None
        if not requests:
            return
        task = asyncio.create_task(self._run_build_batch(requests))
        self._build_batch_tasks.add(task)
        task.add_done_callback(self._build_batch_tasks.discard)

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
                selected = tuple(
                    chapter
                    for chapter in candidates
                    if chapter.build_command.strip().rpartition(" ")[0] == first_prefix
                )
                active_ids = {chapter.id for chapter in selected}
                attempt_requests = tuple(
                    request
                    for request in active
                    if any(chapter.id in active_ids for chapter in request.chapters)
                )
                modes = {request.mode for request in attempt_requests}
                mode = next(iter(modes)) if len(modes) == 1 else "batched"
                owner = max(attempt_requests, key=lambda request: request.priority)
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
                    priority=owner.priority,
                    preemptible=all(request.preemptible for request in attempt_requests),
                    snapshots=attempt_snapshots if capture_snapshots else None,
                )
                if all(result.succeeded for result in attempt_results.values()):
                    results_by_id.update(attempt_results)
                    snapshots_by_id.update(attempt_snapshots)
                    remaining.difference_update(active_ids)
                    finish_ready_requests()
                    continue

                owners = set(self._build_feedback(attempt_results).actionable)
                graph = self._observed_work_unit_graph()
                affected = {
                    chapter_id
                    for chapter_id in active_ids
                    if self._dependency_closure(graph, (chapter_id,)) & owners
                }
                if not affected:
                    affected = active_ids
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
        priority: float = 100.0,
        preemptible: bool = False,
        snapshots: dict[str, ValidatedBuildSnapshot] | None = None,
    ) -> dict[str, ValidationResult]:
        """Execute one deterministic Lake invocation against the coordinator cache."""

        selected = tuple(chapters)
        if not selected:
            return {}
        ids = tuple(chapter.id for chapter in selected)
        label = f"{stage.value} {mode}: " + ", ".join(ids)

        while True:
            await self.control.checkpoint()
            lease = await self.build_queue.acquire(
                priority=priority,
                label=label,
                stage=stage,
                preemptible=preemptible,
            )
            results: dict[str, ValidationResult] = {}
            preempted = False
            source_held = False
            build_workspace = None
            progress_flush: asyncio.Task[None] | None = None

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
                    preemption = asyncio.create_task(lease.preempt_requested.wait())
                    try:
                        done, _ = await asyncio.wait(
                            (validation, preemption), return_when=asyncio.FIRST_COMPLETED
                        )
                    except BaseException:
                        validation.cancel()
                        preemption.cancel()
                        await asyncio.gather(validation, preemption, return_exceptions=True)
                        raise
                    if preemption in done and lease.preempt_requested.is_set():
                        validation.cancel()
                        await asyncio.gather(validation, return_exceptions=True)
                        preempted = True
                        break
                    preemption.cancel()
                    await asyncio.gather(preemption, return_exceptions=True)
                    result = validation.result()
                    results.update((chapter_id, result) for chapter_id in result_ids)
                    await self.state.advance_coordinator_build(
                        work_unit_id=chapter.id,
                        completed=self.state.coordinator_build.total,
                    )
                clean = (
                    not preempted
                    and bool(results)
                    and all(result.succeeded for result in results.values())
                )
                if clean and snapshots is not None:
                    graph = self._observed_work_unit_graph()
                    by_id = {item.id: item for item in self.work_units}
                    captured = {
                        chapter_id: scope_digest(self.config.settings.repo, by_id[chapter_id])
                        for chapter_id in self._dependency_closure(graph, ids)
                    }
                    for chapter in selected:
                        required = self._dependency_closure(graph, (chapter.id,))
                        snapshots[chapter.id] = ValidatedBuildSnapshot(
                            graph=graph,
                            source_digests={
                                chapter_id: captured[chapter_id] for chapter_id in required
                            },
                        )
                await build_workspace.finish(
                    succeeded=clean,
                    publish=publish_if_clean and clean,
                )
                build_workspace = None
            finally:
                try:
                    if progress_flush is not None:
                        if not progress_flush.done():
                            progress_flush.cancel()
                        await asyncio.gather(progress_flush, return_exceptions=True)
                    if build_workspace is not None:
                        await build_workspace.close()
                    if source_held:
                        await self.state.finish_coordinator_build()
                finally:
                    if source_held:
                        self.source_lock.release()
                    self.build_queue.release(lease)
            if not preempted:
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
        by_id = {chapter.id: chapter for chapter in self.work_units}

        def source_location(owner_id: str) -> str:
            owner = by_id[owner_id]
            return f"{owner.source}:{owner.source_span.start_line}-{owner.source_span.end_line}"

        for target_id, result in results.items():
            if result.succeeded:
                continue
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

    async def _discover_all(self) -> bool:
        """Discover every input independently and persist each result as it lands."""

        results = await _gather_cancel_on_error(
            self._discover(chapter) for chapter in self.work_units
        )
        return all(result.succeeded for result in results)

    async def _discover_and_formalize(
        self,
        *,
        progress_event: asyncio.Event | None = None,
        discover: bool = True,
    ) -> bool:
        """Pipeline discovery into dependency-ready formalization without a stage gate."""

        by_id = {chapter.id: chapter for chapter in self.work_units}
        discovery_tasks: dict[str, asyncio.Task[FormalizeOutcome]] = {}
        if discover:
            for chapter in self.work_units:
                if self.force or not self._discovery_is_current(chapter):
                    discovery_tasks[chapter.id] = asyncio.create_task(
                        self._discover(chapter, rerun=self.force)
                    )
        formalize_tasks: dict[str, asyncio.Task[FormalizeOutcome]] = {}
        failed: set[str] = set()

        async def cancel_all() -> None:
            tasks = [*discovery_tasks.values(), *formalize_tasks.values()]
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        try:
            while True:
                graph = self._observed_work_unit_graph()
                succeeded = {
                    chapter_id
                    for chapter_id in by_id
                    if self.state.task(chapter_id, Stage.FORMALIZE).status == TaskStatus.SUCCEEDED
                }
                for chapter_id in graph.order:
                    if (
                        chapter_id in succeeded
                        or chapter_id in failed
                        or chapter_id in formalize_tasks
                    ):
                        continue
                    if self.state.task(chapter_id, Stage.DISCOVER).status != TaskStatus.SUCCEEDED:
                        continue
                    dependencies = graph.dependencies[chapter_id]
                    if dependencies.issubset(succeeded):
                        formalize_tasks[chapter_id] = asyncio.create_task(
                            self._formalize(by_id[chapter_id])
                        )
                    elif dependencies & failed:
                        failed.add(chapter_id)
                        await self.state.set_task(
                            chapter_id,
                            Stage.FORMALIZE,
                            TaskStatus.BLOCKED,
                            "blocked by a failed source dependency formalization",
                        )

                live = [*discovery_tasks.values(), *formalize_tasks.values()]
                if not live:
                    unresolved = set(by_id).difference(succeeded | failed)
                    if unresolved:
                        failed.update(unresolved)
                        await self.state.set_tasks(
                            unresolved,
                            Stage.FORMALIZE,
                            TaskStatus.BLOCKED,
                            "blocked by incomplete discovery or a source dependency cycle",
                        )
                    break

                done, _ = await asyncio.wait(live, return_when=asyncio.FIRST_COMPLETED)
                for chapter_id, task in tuple(discovery_tasks.items()):
                    if task not in done:
                        continue
                    discovery_tasks.pop(chapter_id)
                    if not task.result().succeeded:
                        failed.add(chapter_id)
                        await self.state.set_task(
                            chapter_id,
                            Stage.FORMALIZE,
                            TaskStatus.BLOCKED,
                            "blocked because source discovery failed",
                        )
                for chapter_id, task in tuple(formalize_tasks.items()):
                    if task not in done:
                        continue
                    formalize_tasks.pop(chapter_id)
                    if not task.result().succeeded:
                        failed.add(chapter_id)
                if progress_event is not None:
                    progress_event.set()
        except BaseException:
            await cancel_all()
            raise

        return not failed and all(
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
    ) -> ReviewOutcome:
        if not rerun and self._already_done(chapter, Stage.REVIEW):
            return ReviewOutcome(True, False, complete=True)
        attempt = await self._attempt(
            chapter,
            Stage.REVIEW,
            feedback=feedback,
            queue_detail=(
                "full-scope review of failed-proof findings"
                if feedback
                else "source-faithful editing review"
            ),
        )
        if attempt.agent.capacity_exhausted:
            await self.state.set_task(
                chapter.id,
                Stage.REVIEW,
                TaskStatus.FAILED,
                "model capacity remained unavailable after the configured retries",
            )
            return ReviewOutcome(
                False,
                attempt.agent.changed,
                complete=False,
                run_id=attempt.run.id,
            )
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
            return ReviewOutcome(
                True,
                attempt.agent.changed,
                complete=True,
                run_id=attempt.run.id,
            )
        await self.state.set_task(
            chapter.id,
            Stage.REVIEW,
            TaskStatus.FAILED,
            "editing review failed",
        )
        return ReviewOutcome(
            succeeded,
            attempt.agent.changed,
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
            await self.state.set_tasks(
                targets,
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
                priority=200.0,
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
        feedback = self._build_feedback({chapter.id: result}).actionable
        return feedback or {chapter.id: result.output}

    async def _queue_review_feedback(
        self,
        feedback: dict[str, str],
        *,
        origin: str,
        exclude_from_invalidation: Iterable[str] = (),
    ) -> tuple[str, set[str]]:
        """Persist follow-up work and reopen only its direct owners."""

        request_id, created = await self.state.enqueue_proof_review_request(
            feedback,
            origin_run_id=origin,
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
        proof_request_ids: tuple[str, ...] = (),
    ) -> bool:
        """Run at most five edit/rebuild cycles for one reviewable chapter."""

        review_generation = self._review_invalidation_generation(chapter.id)
        if self.state.task(chapter.id, Stage.REVIEW).status == TaskStatus.SUCCEEDED:
            return True
        graph = self._observed_work_unit_graph()
        request_ids = list(proof_request_ids)
        review_feedback = feedback

        async def route_feedback(items: dict[str, str], *, origin: str) -> bool:
            nonlocal review_feedback
            if not items:
                return True
            request_id, _ = await self._queue_review_feedback(
                items,
                origin=origin,
                exclude_from_invalidation={chapter.id},
            )
            if chapter.id in items:
                if request_id not in request_ids:
                    request_ids.append(request_id)
                block = items[chapter.id]
                if block not in review_feedback:
                    review_feedback = f"{review_feedback}\n\n{block}" if review_feedback else block
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
        was_clean = chapter.id in self._retain_formalize_clean(graph, records)
        if not was_clean and not rerun:
            build_feedback = await self._review_build(chapter)
            if build_feedback and not await route_feedback(
                build_feedback,
                origin=f"review-build:{chapter.id}:{uuid4().hex[:12]}",
            ):
                return False

        maximum = min(self.config.stages[Stage.REVIEW].max_rounds, 5)
        while rounds_used[chapter.id] < maximum:
            rounds_used[chapter.id] += 1
            review_rerun = rerun or rounds_used[chapter.id] > 1
            finding_guided = bool(review_feedback)
            outcome = (
                await self._review_once(
                    chapter,
                    rerun=review_rerun,
                    feedback=review_feedback,
                )
                if review_feedback
                else await self._review_once(chapter, rerun=review_rerun)
            )
            review_feedback = ""
            if not outcome.succeeded:
                return False
            build_feedback: dict[str, str] = {}
            if outcome.changed:
                build_feedback = await self._review_build(chapter)
                if build_feedback and not await route_feedback(
                    build_feedback,
                    origin=f"review-build:{outcome.run_id or uuid4().hex[:12]}",
                ):
                    return False
            if review_feedback:
                if rounds_used[chapter.id] >= maximum:
                    await self.state.set_task(
                        chapter.id,
                        Stage.REVIEW,
                        TaskStatus.FAILED,
                        f"review follow-up remained unresolved after {maximum} cycles",
                    )
                    return False
                continue
            if not outcome.complete:
                if not outcome.changed and not build_feedback:
                    await self.state.set_task(
                        chapter.id,
                        Stage.REVIEW,
                        TaskStatus.FAILED,
                        "review was incomplete and supplied no actionable follow-up",
                    )
                    return False
                if rounds_used[chapter.id] >= maximum:
                    await self.state.set_task(
                        chapter.id,
                        Stage.REVIEW,
                        TaskStatus.FAILED,
                        f"review remained incomplete after {maximum} cycles",
                    )
                    return False
                continue
            if finding_guided:
                return await self._complete_review(
                    chapter,
                    "targeted review completed with no pending findings",
                    expected_generation=review_generation,
                    proof_request_ids=request_ids,
                )
            if not outcome.changed:
                return await self._complete_review(
                    chapter,
                    "editing review found no actionable issues",
                    expected_generation=review_generation,
                    proof_request_ids=request_ids,
                )
        return await self._complete_review(
            chapter,
            f"review/rebuild cap reached after {maximum} cycles",
            expected_generation=review_generation,
            proof_request_ids=request_ids,
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
            await self.state.set_tasks(
                by_id,
                Stage.REVIEW,
                TaskStatus.FAILED,
                str(error),
            )
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
        proof_tasks: dict[str, asyncio.Task[bool]] = {}
        failed_rebuilds: set[str] = set()
        persisted_clean = self.state.formalize_graph.get("clean", {})
        clean = self._retain_formalize_clean(
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

        async with self.state.batch():
            if quarantined_ids:
                await self.state.set_tasks(
                    quarantined_ids,
                    Stage.REVIEW,
                    TaskStatus.FAILED,
                    "formalization failed; quarantined from review",
                )
                await self.state.set_tasks(
                    quarantined_ids,
                    Stage.PROVE,
                    TaskStatus.BLOCKED,
                    "formalization failed; quarantined from proof",
                )
            for chapter_id in initial_graph.order:
                if chapter_id not in reviewed:
                    if not formalize_ready(chapter_id):
                        await self.state.set_task(
                            chapter_id,
                            Stage.REVIEW,
                            TaskStatus.PENDING,
                            "waiting for clean formalization",
                        )
                    else:
                        required_reviews = self._dependency_closure(
                            initial_graph, (chapter_id,)
                        ).difference({chapter_id})
                        missing = required_reviews.difference(reviewed)
                        if missing:
                            await self.state.set_task(
                                chapter_id,
                                Stage.REVIEW,
                                TaskStatus.PENDING,
                                "waiting: " + ", ".join(sorted(missing)),
                            )
                if chapter_id not in reviewed:
                    await self.state.set_task(
                        chapter_id,
                        Stage.PROVE,
                        TaskStatus.PENDING,
                        "waiting for successful review",
                    )

        async def cancel_all() -> None:
            tasks = [handle.task for handle in review_tasks.values()]
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
                    await self.state.set_tasks(
                        by_id,
                        Stage.REVIEW,
                        TaskStatus.FAILED,
                        str(error),
                    )
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
                        async with self.state.batch():
                            await self.state.set_tasks(
                                failed_formalizations,
                                Stage.REVIEW,
                                TaskStatus.FAILED,
                                "formalization did not complete",
                            )
                            if prove:
                                await self.state.set_tasks(
                                    failed_formalizations,
                                    Stage.PROVE,
                                    TaskStatus.BLOCKED,
                                    "blocked because formalization did not complete",
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
                cancelled_reviews: list[asyncio.Task[bool]] = []
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

                for chapter_id in graph.order:
                    review_task = self.state.task(chapter_id, Stage.REVIEW)
                    # A forced pipeline run is not itself evidence that this node has
                    # already been reviewed. Only actual prior/active review work may
                    # bypass dependency-review ordering.
                    rereview = chapter_id in attempted or review_task.rounds > 0
                    required_reviews = self._dependency_closure(graph, (chapter_id,)).difference(
                        {chapter_id}
                    )
                    if (
                        chapter_id not in reviewed
                        and chapter_id not in review_failures
                        and chapter_id not in review_blocked
                        and chapter_id not in review_tasks
                        and chapter_id not in proof_tasks
                        and chapter_id not in rebuild_tasks
                        and (rereview or formalize_ready(chapter_id))
                        and (rereview or required_reviews.issubset(reviewed))
                    ):
                        dependencies = graph.dependencies[chapter_id]
                        proof_feedback, proof_request_ids = self._proof_review_feedback(chapter_id)
                        review_rerun = rerun or chapter_id in attempted or review_task.rounds > 0
                        await self.state.set_task(
                            chapter_id,
                            Stage.REVIEW,
                            TaskStatus.RUNNING,
                            (
                                "targeted re-review queued"
                                if rereview
                                else "waiting for dependency-ordered coordinator build"
                            ),
                        )
                        review_operation = (
                            self._review_chapter_to_clean(
                                by_id[chapter_id],
                                rounds_used,
                                rerun=review_rerun,
                                feedback=proof_feedback,
                                proof_request_ids=proof_request_ids,
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
                        ):
                            proof_tasks[chapter_id] = asyncio.create_task(
                                self._prove(by_id[chapter_id], defer_review=True)
                            )

                live_tasks = [handle.task for handle in review_tasks.values()]
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
                        await self.state.set_tasks(
                            unresolved,
                            Stage.REVIEW,
                            TaskStatus.BLOCKED,
                            "blocked by a failed prerequisite review; unrelated branches completed",
                        )
                        if prove:
                            await self.state.set_tasks(
                                unresolved | review_failures,
                                Stage.PROVE,
                                TaskStatus.BLOCKED,
                                "blocked because statement review did not complete",
                            )
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
                    succeeded = handle.task.result()
                    if not succeeded:
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
                    proof_succeeded = proof_tasks.pop(chapter_id).result()
                    if proof_succeeded:
                        proof_results[chapter_id] = True
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

                    cancelled: list[asyncio.Task[bool]] = []
                    for invalidated_id in invalidated:
                        if handle := review_tasks.pop(invalidated_id, None):
                            handle.task.cancel()
                            cancelled.append(handle.task)
                        reviewed.discard(invalidated_id)
                        rounds_used[invalidated_id] = 0
                    await asyncio.gather(*cancelled, return_exceptions=True)
        finally:
            await cancel_all()

        return (
            not review_failures
            and not review_blocked
            and (not prove or all(proof_results.values()))
        )

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
                priority=0.0,
                preemptible=True,
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
            )
        return validation

    async def _rebuild_dirty_chapter(self, chapter: WorkUnitLike) -> bool:
        """Refresh one invalidated exact build while its chapter has no agent."""

        validation = await self._refresh_stale_proof_build(chapter)
        return validation.succeeded

    async def _prove(self, chapter: WorkUnitLike, *, defer_review: bool = False) -> bool:
        initial_feedback = ""
        build_fresh = False
        if not self.force:
            graph = self._observed_work_unit_graph()
            persisted = self.state.formalize_graph.get("clean", {})
            clean = self._retain_formalize_clean(
                graph,
                persisted if isinstance(persisted, dict) else {},
            )
            proof_task = self.state.task(chapter.id, Stage.PROVE)
            record = clean.get(chapter.id)
            if (
                proof_task.status == TaskStatus.SUCCEEDED
                and isinstance(record, dict)
                and proof_task.source_digest == record.get("source_digest")
            ):
                await self._close_previously_satisfied_upstream_requests(
                    chapter,
                    build_fresh=True,
                )
                return True
            files = scoped_files(self.config.settings.repo, chapter)
            if files:
                placeholders = count_placeholders(self.config.settings.repo, chapter)
                build_fresh = isinstance(record, dict)
                if not build_fresh:
                    revalidation = await self._refresh_stale_proof_build(chapter)
                    if not revalidation.succeeded:
                        await self.state.set_task(
                            chapter.id,
                            Stage.PROVE,
                            TaskStatus.PENDING,
                            "current sources failed coordinator build refresh",
                        )
                        initial_feedback = (
                            "Coordinator validation of the current sources failed before proof "
                            "work:\n" + revalidation.output
                        )
                        if placeholders == 0:
                            return False
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
                    await self.state.set_task(
                        chapter.id,
                        Stage.PROVE,
                        TaskStatus.SUCCEEDED,
                        "placeholder-free sources validated without an agent",
                        source_digest=scope_digest(self.config.settings.repo, chapter),
                    )
                    return True
        proof_maximum = self.config.stages[Stage.PROVE].max_rounds
        feedback = initial_feedback
        feedback_ledger: deque[str] = deque(maxlen=PROOF_FEEDBACK_ROUNDS)
        if initial_feedback:
            feedback_ledger.append(initial_feedback)
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
                TaskStatus.BLOCKED,
                "upstream request requires manual escalation: " + ", ".join(escalated),
            )
            return False
        targeted_request_ids = answered_ids
        if targeted_request_ids:
            feedback = self._upstream_retry_feedback(
                targeted_request_ids,
                _bounded_proof_feedback(feedback_ledger),
            )
        while proof_round < proof_maximum or targeted_request_ids:
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
            attempt = await self._attempt(
                chapter,
                Stage.PROVE,
                feedback=feedback,
                queue_detail=(
                    "targeted downstream retry for upstream request(s): "
                    + ", ".join(targeted_request_ids)
                    if targeted_retry
                    else f"proof round {proof_round + 1}/{proof_maximum}"
                ),
                role=DOWNSTREAM_RETRY_ROLE if targeted_retry else "",
                request_ids=targeted_request_ids,
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
                    TaskStatus.BLOCKED if targeted_retry else TaskStatus.FAILED,
                    (
                        "targeted downstream retry requires manual escalation"
                        if targeted_retry
                        else "model capacity remained unavailable after the configured retries"
                    ),
                )
                return False
            proof_round += 1
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
                        else "targeted retry did not validate: " + attempt.validation.output[-4000:]
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
                        f"Targeted downstream retry {proof_round}:\n{attempt.feedback()}"
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
                        TaskStatus.BLOCKED,
                        "targeted downstream retry did not prove: " + ", ".join(sorted(unresolved)),
                    )
                    return False
            if (
                attempt.agent.succeeded
                and attempt.validation.succeeded
                and attempt.agent.placeholders == 0
            ):
                await self.state.set_task(
                    chapter.id,
                    Stage.PROVE,
                    TaskStatus.SUCCEEDED,
                    "no placeholders and chapter elaborates",
                    source_digest=scope_digest(self.config.settings.repo, chapter),
                )
                return True

            feedback_ledger.append(f"Proof attempt {proof_round}:\n{attempt.feedback()}")
            feedback = _bounded_proof_feedback(feedback_ledger)
            upstream_request_ids = await self._record_upstream_requests(
                chapter,
                attempt.run,
                attempt.agent.report,
                previous_attempts=feedback,
            )
            if bool(attempt.agent.report.get("failed_attempts")):
                await self.state.set_task(
                    chapter.id,
                    Stage.PROVE,
                    TaskStatus.PENDING,
                    "proof left checked failures; waiting for independent review",
                )
                if not defer_review:
                    await self._queue_proof_review(
                        chapter,
                        attempt.agent.report,
                        origin_run_id=attempt.run.id,
                    )
                return False
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
                        TaskStatus.BLOCKED,
                        "upstream request requires manual escalation: " + ", ".join(escalated),
                    )
                    return False
                targeted_request_ids = answered_ids
                feedback = self._upstream_retry_feedback(
                    targeted_request_ids,
                    feedback,
                )
                continue
            placeholders = attempt.agent.placeholders
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
                return False

        await self.state.set_task(
            chapter.id,
            Stage.PROVE,
            TaskStatus.FAILED,
            f"proof pass did not converge in {proof_maximum} rounds",
        )
        return False

    async def run_stage(self, stage: Stage) -> bool:
        if stage is Stage.DISCOVER:
            return await self._discover_all()
        if stage is Stage.FORMALIZE:
            return await self._discover_and_formalize(discover=True)
        if stage is Stage.REVIEW:
            return await self._review_until_clean()
        return await self._review_tree(prove=True)

    async def run_pipeline(self) -> bool:
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
