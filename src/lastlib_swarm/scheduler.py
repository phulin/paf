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

from lastlib_swarm.codex import (
    AgentResult,
    CodexExecutor,
    ValidationResult,
    count_placeholders,
    scope_digest,
    scoped_files,
    validate,
)
from lastlib_swarm.coordination import CoordinatorBuildQueue, PriorityLimiter
from lastlib_swarm.corpus import (
    ChapterImportGraph,
    build_chapter_import_graph,
    build_corpus_schedule,
    scheduling_snapshot,
)
from lastlib_swarm.diagnostics import unexpected_lean_warnings
from lastlib_swarm.isolation import create_isolation
from lastlib_swarm.models import Chapter, PipelineConfig, Stage
from lastlib_swarm.scope import ScopeMatcher
from lastlib_swarm.state import RunRecord, StateStore, TaskStatus


async def _gather_cancel_on_error(
    operations: Iterable[Coroutine[Any, Any, bool]],
) -> list[bool]:
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
    feedback_by_owner: dict[str, str]
    complete: bool = True
    run_id: str = ""


@dataclass(frozen=True)
class BuildDiagnostics:
    actionable: dict[str, str]
    deferred_owner_ids: tuple[str, ...]


@dataclass(frozen=True)
class ValidatedBuildSnapshot:
    graph: ChapterImportGraph
    source_digests: dict[str, str]


@dataclass
class RunningFixupAgent:
    chapter: Chapter
    run: RunRecord
    workspace: Any
    task: asyncio.Task[AgentResult]
    dependency_certificates: dict[str, str]
    feedback: str
    source_lock_held: bool = False


@dataclass(frozen=True)
class RunningFixupStage:
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

    def stop(self) -> None:
        self.stopping = True
        self.paused = False
        self._gate.set()


def scaffold_directories(config: PipelineConfig, chapters: Iterable[Chapter]) -> tuple[str, ...]:
    """Create chapter directories deterministically without creating Lean files."""

    created: list[str] = []
    for chapter in chapters:
        directory = config.settings.repo / chapter.lean_root / chapter.chapter_path
        if not directory.is_dir():
            directory.mkdir(parents=True, exist_ok=True)
            created.append(directory.relative_to(config.settings.repo).as_posix())
    return tuple(created)


class Orchestrator:
    def __init__(
        self,
        config: PipelineConfig,
        state: StateStore,
        *,
        chapters: Iterable[Chapter] | None = None,
        force: bool = False,
        control: RunControl | None = None,
    ) -> None:
        self.config = config
        self.state = state
        self.chapters = tuple(chapters if chapters is not None else config.chapters)
        self.force = force
        self.control = control or RunControl()
        self.executor = CodexExecutor(config, state)
        self.isolation = create_isolation(config.settings)
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
        selected_books = {chapter.book_id for chapter in self.chapters}
        self.statement_schedule = build_corpus_schedule(
            config.books,
            self.chapters,
            phase="statements",
            selected_books=selected_books,
        )
        self.proof_schedule = build_corpus_schedule(
            config.books,
            self.chapters,
            phase="proofs",
            selected_books=selected_books,
        )
        self.state.scheduling = self.scheduling_snapshot()
        self.agent_slots = PriorityLimiter(config.settings.max_agents)
        self.build_queue = CoordinatorBuildQueue()
        # Snapshot creation and scoped source integration need a short
        # consistency barrier with main-worktree builds. Unlike build_queue,
        # this lock is never held for an overlay agent's editing lifetime or
        # for a proof validation outside a coordinator build.
        self.source_lock = asyncio.Lock()
        self._fixup_graph_lock = asyncio.Lock()
        self._invalidated_reviews: set[str] = set()
        self._quiescent_review_rebuilds: set[str] = set()
        self._proof_rechecks: set[str] = set()
        self._review_invalidation_generations: dict[str, int] = {}
        self._review_generation_lock = asyncio.Lock()

    def scheduling_snapshot(self) -> dict[str, object]:
        return scheduling_snapshot(self.statement_schedule, self.proof_schedule)

    async def prepare(self) -> None:
        await self.state.load_or_create()
        self.scaffold()
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

    async def shutdown(self) -> None:
        try:
            await self.isolation.close()
        finally:
            await self.state.close()

    def _already_done(self, chapter: Chapter, stage: Stage) -> bool:
        return not self.force and self.state.task(chapter.id, stage).status == TaskStatus.SUCCEEDED

    def scaffold(self) -> None:
        """Create configured chapter directories without creating Lean files."""

        scaffold_directories(self.config, self.chapters)

    def _observed_chapter_graph(self) -> ChapterImportGraph:
        return build_chapter_import_graph(self.config.settings.repo, self.chapters)

    @staticmethod
    def _fixup_certificate(
        source: str,
        dependencies: frozenset[str],
        clean: dict[str, dict[str, Any]],
    ) -> str:
        digest = hashlib.sha256()
        digest.update(source.encode())
        for dependency in sorted(dependencies):
            digest.update(b"\0")
            digest.update(dependency.encode())
            digest.update(b"\0")
            digest.update(str(clean[dependency]["certificate"]).encode())
        return digest.hexdigest()

    def _retain_fixup_clean(
        self,
        graph: ChapterImportGraph,
        records: dict[str, Any],
    ) -> dict[str, dict[str, Any]]:
        """Retain build certificates whose sources and observed prerequisites still match."""

        by_id = {chapter.id: chapter for chapter in self.chapters}
        retained: dict[str, dict[str, Any]] = {}
        for chapter_id in graph.order:
            required = graph.dependencies[chapter_id]
            if not required.issubset(retained):
                continue
            record = records.get(chapter_id)
            if not isinstance(record, dict):
                continue
            source = scope_digest(self.config.settings.repo, by_id[chapter_id])
            certificate = self._fixup_certificate(source, required, retained)
            if record.get("source_digest") == source and record.get("certificate") == certificate:
                retained[chapter_id] = dict(record)
        return retained

    @staticmethod
    def _dependency_closure(graph: ChapterImportGraph, chapter_ids: Iterable[str]) -> set[str]:
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
    def _successor_closure(graph: ChapterImportGraph, chapter_ids: Iterable[str]) -> set[str]:
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
            graph = self._observed_chapter_graph()
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

            by_id = {item.id: item for item in self.chapters}
            current = {
                chapter_id: scope_digest(self.config.settings.repo, by_id[chapter_id])
                for chapter_id in required
            }
            if any(captured[chapter_id] != digest for chapter_id, digest in current.items()):
                return False

            persisted = self.state.fixup_graph.get("clean", {})
            records = persisted if isinstance(persisted, dict) else {}
            clean = self._retain_fixup_clean(graph, records)
            build_generation = int(self.state.fixup_graph.get("build_generation", 0))
            for chapter_id in graph.order:
                if chapter_id not in required:
                    continue
                source = captured[chapter_id]
                certificate = self._fixup_certificate(
                    source,
                    graph.dependencies[chapter_id],
                    clean,
                )
                record = clean.get(chapter_id)
                if (
                    isinstance(record, dict)
                    and record.get("source_digest") == source
                    and record.get("certificate") == certificate
                ):
                    continue
                build_generation += 1
                clean[chapter_id] = {
                    "source_digest": source,
                    "certificate": certificate,
                    "build_generation": build_generation,
                }
            await self._save_fixup_graph(
                graph,
                clean,
                build_generation=build_generation,
            )
            return True

    async def _publish_validated_build(
        self,
        chapter: Chapter,
        snapshot: ValidatedBuildSnapshot,
    ) -> bool:
        return await self._publish_validated_builds({chapter.id: snapshot})

    @staticmethod
    def _invalidate_fixup_descendants(
        graph: ChapterImportGraph,
        clean: dict[str, dict[str, Any]],
        chapter_ids: Iterable[str],
    ) -> set[str]:
        invalidated = Orchestrator._successor_closure(graph, chapter_ids)
        for chapter_id in invalidated:
            clean.pop(chapter_id, None)
        return invalidated

    async def _invalidate_build_records(self, chapter_ids: Iterable[str]) -> set[str]:
        """Mark an edited source closure stale before any verification is queued."""

        graph = self._observed_chapter_graph()
        persisted = self.state.fixup_graph.get("clean", {})
        clean = self._retain_fixup_clean(graph, persisted if isinstance(persisted, dict) else {})
        invalidated = self._invalidate_fixup_descendants(graph, clean, chapter_ids)
        await self._save_fixup_graph(
            graph,
            clean,
            build_generation=int(self.state.fixup_graph.get("build_generation", 0)),
            invalidated=invalidated,
        )
        return invalidated

    async def _save_fixup_graph(
        self,
        graph: ChapterImportGraph,
        clean: dict[str, dict[str, Any]],
        *,
        build_generation: int,
        invalidated: Iterable[str] = (),
    ) -> int:
        async with self._fixup_graph_lock:
            explicitly_invalidated = set(invalidated)
            previous = self.state.fixup_graph
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
            retained = self._retain_fixup_clean(graph, clean)
            clean.clear()
            clean.update(retained)
            edges = [list(edge) for edge in graph.edges]
            revision = int(previous.get("revision", 0)) if isinstance(previous, dict) else 0
            if previous.get("algorithm") != "observed-lean-imports" or previous_edges != edges:
                revision += 1
            build_generation = max(
                build_generation,
                int(previous.get("build_generation", 0)) if isinstance(previous, dict) else 0,
            )
            self.state.fixup_graph = graph.snapshot() | {
                "revision": revision,
                "build_generation": build_generation,
                "clean": clean,
            }
            await self.state.save()
            return revision

    def _scope_exists(self, chapter: Chapter) -> bool:
        return ScopeMatcher(chapter.scope).has_match_for_each_pattern(self.config.settings.repo)

    async def _attempt(
        self,
        chapter: Chapter,
        stage: Stage,
        *,
        feedback: str = "",
        queue_detail: str = "",
    ) -> Attempt:
        await self.state.set_task(
            chapter.id,
            stage,
            TaskStatus.RUNNING,
            queue_detail or f"queued for {stage.value} agent",
        )
        await self.control.checkpoint()
        schedule = (
            self.statement_schedule
            if stage in (Stage.FORMALIZE, Stage.FIXUP, Stage.REVIEW)
            else self.proof_schedule
        )
        await self.agent_slots.acquire(schedule.priority(chapter.book_id))
        slot_held = True
        run = None
        workspace = None
        source_held = False
        try:
            run = await self.state.start_run(chapter.id, stage)
            if self.isolation.name == "shared":
                await self.source_lock.acquire()
                source_held = True
                workspace = await self.isolation.acquire(run.id)
            else:
                workspace = await self.isolation.acquire(run.id)
            agent = await self.executor.run(
                chapter,
                stage,
                run,
                feedback=feedback,
                workspace_root=workspace.root,
            )
            self.agent_slots.release()
            slot_held = False
            # Agent capacity covers live Codex processes, not integration or a
            # potentially preempted coordinator build queued after they exit.
            isolated = await workspace.collect(
                chapter,
                integration_lock=None if source_held else self.source_lock,
            )
            await workspace.close()
            workspace = None
            if source_held:
                self.source_lock.release()
                source_held = False
            if isolated.accepted and agent.changed:
                invalidated_builds = await self._invalidate_build_records((chapter.id,))
                if stage is Stage.REVIEW:
                    self._proof_rechecks.update(invalidated_builds)
                elif stage is Stage.PROVE:
                    self._proof_rechecks.update(invalidated_builds.difference({chapter.id}))
            if isolated.accepted:
                if stage is Stage.PROVE:
                    snapshots: dict[str, ValidatedBuildSnapshot] = {}
                    validation = (
                        await self._build_chapters(
                            (chapter,),
                            publish_if_clean=True,
                            mode="proof-certification",
                            stage=Stage.PROVE,
                            priority=0.0,
                            preemptible=True,
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
                else:
                    validation = ValidationResult(
                        True,
                        0,
                        "validation deferred to the coordinator fixup loop",
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
            if run is not None and run.status == TaskStatus.RUNNING:
                detail = str(error) or type(error).__name__
                await self.state.finish_run(
                    run,
                    status=TaskStatus.FAILED,
                    isolation={
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
        assert run is not None
        return Attempt(agent=agent, validation=validation, run=run)

    async def _formalize(self, chapter: Chapter, *, rerun: bool = False) -> FormalizeOutcome:
        if self._already_done(chapter, Stage.FORMALIZE):
            return FormalizeOutcome(True)
        if not rerun and not self.force and self._scope_exists(chapter):
            await self.state.set_task(
                chapter.id,
                Stage.FORMALIZE,
                TaskStatus.SUCCEEDED,
                "existing chapter files skipped",
            )
            return FormalizeOutcome(True)
        attempt = await self._attempt(
            chapter,
            Stage.FORMALIZE,
            queue_detail="single optimistic drafting pass",
        )
        complete = bool(attempt.agent.report.get("complete"))
        if attempt.agent.succeeded and attempt.validation.succeeded and complete:
            await self.state.set_task(
                chapter.id,
                Stage.FORMALIZE,
                TaskStatus.SUCCEEDED,
                "optimistic chapter draft completed",
            )
            return FormalizeOutcome(True)
        if attempt.agent.capacity_exhausted:
            await self.state.set_task(
                chapter.id,
                Stage.FORMALIZE,
                TaskStatus.FAILED,
                "model capacity remained unavailable after the configured retries",
            )
            return FormalizeOutcome(False)
        await self.state.set_task(
            chapter.id,
            Stage.FORMALIZE,
            TaskStatus.FAILED,
            (
                "formalizer reported an incomplete chapter draft"
                if attempt.agent.succeeded and attempt.validation.succeeded
                else "single drafting attempt failed"
            ),
        )
        return FormalizeOutcome(False)

    def _chapter_identifiers(self, chapter: Chapter) -> tuple[str, ...]:
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
        for chapter in self.chapters:
            root = (chapter.lean_root / chapter.chapter_path).as_posix()
            lean_prefix = self.config.settings.lean_project.as_posix().rstrip("/") + "/"
            roots = (root, root.removeprefix(lean_prefix))
            if any(
                normalized == f"{item}.lean" or normalized.startswith(f"{item}/") for item in roots
            ):
                owners.append(chapter.id)
        return tuple(dict.fromkeys(owners))

    def _route_review_findings(
        self,
        chapter: Chapter,
        report: dict[str, Any],
    ) -> dict[str, str]:
        """Route structured review findings to the chapters owning their requested edit paths."""

        routed: dict[str, dict[str, None]] = {}
        findings = report.get("fixup_findings")
        if isinstance(findings, list):
            for finding in findings:
                if not isinstance(finding, dict):
                    continue
                description = finding.get("description")
                paths = finding.get("owner_paths")
                if not isinstance(description, str) or not description.strip():
                    continue
                if not isinstance(paths, list):
                    paths = []
                owner_paths = tuple(
                    path for path in paths if isinstance(path, str) and path.strip()
                )
                paths_by_owner: dict[str, dict[str, None]] = {}
                for path in owner_paths:
                    owners = self._path_owner_ids(path)
                    # Preserve compatibility with repository-wide findings that cannot be assigned
                    # from a chapter scope. The reviewing chapter will retain them as blockers.
                    if not owners:
                        owners = (chapter.id,)
                    for owner in owners:
                        paths_by_owner.setdefault(owner, {})[path] = None
                if not paths_by_owner:
                    paths_by_owner[chapter.id] = {}
                for owner, owned_paths in paths_by_owner.items():
                    paths_block = "\n".join(f"- `{path}`" for path in owned_paths)
                    block = (
                        f"Review finding reported while auditing `{chapter.id}`:\n"
                        f"{description.strip()}"
                    )
                    if paths_block:
                        block += f"\nRequested edit paths owned by `{owner}`:\n{paths_block}"
                    routed.setdefault(owner, {})[block] = None

        if routed:
            return {owner: "\n\n".join(blocks) for owner, blocks in routed.items()}
        return {}

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
        chapter: Chapter,
        report: dict[str, Any],
        *,
        origin_run_id: str,
    ) -> set[str]:
        """Durably hand proof findings to full-scope statement reviews."""

        routed = self._route_review_findings(chapter, report)
        if not routed:
            return set()
        origin_blocks = tuple(dict.fromkeys(routed.values()))
        origin_feedback = (
            f"Proof of `{chapter.id}` failed with possible statement or interface defects. "
            "Evaluate these findings while re-reviewing the complete assigned scope:\n\n"
            + "\n\n".join(origin_blocks)
        )
        feedback = dict(routed)
        feedback[chapter.id] = origin_feedback
        _, created = await self.state.enqueue_proof_review_request(
            feedback,
            origin_run_id=origin_run_id,
        )
        targets = {chapter.id, *routed}
        if not created:
            return targets
        return await self._invalidate_reviews(
            targets,
            detail="review invalidated by failed-proof findings",
            quiesce_downstream=True,
        )

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
        for chapter in self.chapters:
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

    async def _recover_proof_review_requests(self) -> None:
        """Recover the proof-to-review handoff if a process died between its durable steps."""

        persisted_origins = {
            value.get("origin_run_id")
            for value in self.state.proof_review_requests.values()
            if isinstance(value, dict)
        }
        for chapter in self.chapters:
            proof_runs = self.state.task(chapter.id, Stage.PROVE).runs
            if not proof_runs:
                continue
            run = proof_runs[-1]
            self.state.load_run_details(run)
            report = run.report if isinstance(run.report, dict) else {}
            if not report.get("fixup_findings") or run.id in persisted_origins:
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

        by_id = {chapter.id: chapter for chapter in self.chapters}
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
            for chapter in self.chapters
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
        for chapter in self.chapters:
            for identifier in self._chapter_identifiers(chapter):
                pattern = rf"(?<![A-Za-z0-9_]){re.escape(identifier)}(?![A-Za-z0-9_])"
                if re.search(pattern, diagnostic.text):
                    owners.append(chapter.id)
                    break
        return tuple(dict.fromkeys(owners))

    def _target_error_count(self, chapter: Chapter, output: str) -> int:
        """Count streamed error headers assigned to the current build target."""

        return sum(
            diagnostic.severity == "error" and chapter.id in self._diagnostic_owner_ids(diagnostic)
            for diagnostic in _lean_diagnostics(output)
        )

    async def _build_chapters(
        self,
        chapters: Iterable[Chapter],
        *,
        publish_if_clean: bool,
        mode: str = "targeted",
        iteration: int = 1,
        maximum_iterations: int = 1,
        stage: Stage = Stage.FIXUP,
        priority: float = 100.0,
        preemptible: bool = False,
        snapshots: dict[str, ValidatedBuildSnapshot] | None = None,
    ) -> dict[str, ValidationResult]:
        """Build a deterministic target batch against the coordinator-owned cache."""

        selected = tuple(chapters)
        if not selected:
            return {}
        ids = tuple(chapter.id for chapter in selected)
        label = f"{stage.value} {mode}: " + ", ".join(ids)

        while True:
            await self.control.checkpoint()
            await self.state.set_tasks(
                ids,
                stage,
                TaskStatus.RUNNING,
                (
                    "queued for coordinator verification"
                    if stage is Stage.REVIEW
                    else f"queued for {mode} coordinator build"
                ),
            )
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
            try:
                await self.source_lock.acquire()
                source_held = True
                build_workspace = await self.isolation.acquire_build(label)
                async with self.state.batch():
                    await self.state.set_tasks(
                        ids,
                        stage,
                        TaskStatus.RUNNING,
                        f"{mode} coordinator build {iteration}/{maximum_iterations}",
                    )
                    await self.state.start_coordinator_build(
                        mode=mode,
                        stage=stage,
                        iteration=iteration,
                        maximum_iterations=maximum_iterations,
                        total=len(selected),
                    )
                for index, chapter in enumerate(selected):
                    await self.state.advance_coordinator_build(
                        chapter_id=chapter.id,
                        completed=index,
                        command=chapter.build_command,
                    )

                    def append_output(output: str, *, current: Chapter = chapter) -> None:
                        self.state.append_coordinator_build_output(
                            output,
                            error_count=self._target_error_count(current, output),
                        )

                    validation = asyncio.create_task(
                        validate(
                            self.config,
                            chapter,
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
                    results[chapter.id] = validation.result()
                    await self.state.advance_coordinator_build(
                        chapter_id=chapter.id,
                        completed=index + 1,
                    )
                clean = (
                    not preempted
                    and bool(results)
                    and all(result.succeeded for result in results.values())
                )
                if clean and snapshots is not None:
                    graph = self._observed_chapter_graph()
                    by_id = {item.id: item for item in self.chapters}
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
                if not preempted:
                    await self.state.set_tasks(
                        ids,
                        stage,
                        TaskStatus.RUNNING,
                        "coordinator build finished; reconciling result",
                    )
            finally:
                try:
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
            self.chapters,
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
        by_id = {chapter.id: chapter for chapter in self.chapters}
        for target_id, result in results.items():
            if result.succeeded:
                continue
            routed = False
            for diagnostic in _lean_diagnostics(result.output):
                owners = self._diagnostic_owner_ids(diagnostic)
                if not owners:
                    continue
                routed = True
                block = f"Coordinator diagnostic:\n{diagnostic.text}"
                for owner in owners:
                    feedback.setdefault(owner, {})[block] = None

            # A truncated Lake log can retain its failed-module summary while
            # still retaining an unrelated warning. Always route the precise
            # failed module as well as any source-located diagnostics.
            for module in _failed_modules(result.output):
                owners = self._module_owner_ids(module)
                if not owners:
                    continue
                routed = True
                block = f"Coordinator reported failed module `{module}`."
                for owner in owners:
                    feedback.setdefault(owner, {})[block] = None

            if not routed:
                block = (
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

    async def _start_fixup_agent(
        self,
        chapter: Chapter,
        feedback: str,
        dependency_certificates: dict[str, str],
    ) -> RunningFixupAgent:
        """Start an isolated fixup agent without waiting for or merging its result."""

        maximum = self.config.stages[Stage.FIXUP].max_rounds
        task = self.state.task(chapter.id, Stage.FIXUP)
        await self.state.set_task(
            chapter.id,
            Stage.FIXUP,
            TaskStatus.RUNNING,
            f"fixup run {task.rounds + 1} (repair-cycle cap {maximum})",
        )
        await self.control.checkpoint()
        await self.agent_slots.acquire(self.statement_schedule.priority(chapter.book_id))
        run = None
        workspace = None
        source_held = False
        try:
            run = await self.state.start_run(chapter.id, Stage.FIXUP)
            if self.isolation.name == "shared":
                await self.source_lock.acquire()
                source_held = True
                workspace = await self.isolation.acquire(run.id)
            else:
                workspace = await self.isolation.acquire(run.id)

            async def execute() -> AgentResult:
                try:
                    return await self.executor.run(
                        chapter,
                        Stage.FIXUP,
                        run,
                        feedback=feedback,
                        workspace_root=workspace.root,
                    )
                finally:
                    self.agent_slots.release()

            return RunningFixupAgent(
                chapter=chapter,
                run=run,
                workspace=workspace,
                task=asyncio.create_task(execute()),
                dependency_certificates=dependency_certificates,
                feedback=feedback,
                source_lock_held=source_held,
            )
        except BaseException as error:
            self.agent_slots.release()
            if workspace is not None:
                await workspace.close()
            if source_held:
                self.source_lock.release()
            if run is not None and run.status == TaskStatus.RUNNING:
                detail = str(error) or type(error).__name__
                await self.state.finish_run(
                    run,
                    status=TaskStatus.FAILED,
                    isolation={
                        "accepted": False,
                        "error": f"orchestration failed before agent start: {detail}",
                    },
                )
            raise

    async def _integrate_fixup_agent(self, handle: RunningFixupAgent) -> Attempt | None:
        """Merge one completed agent while no other source integration can interleave."""

        workspace = handle.workspace
        try:
            agent = await handle.task
            isolated = await workspace.collect(
                handle.chapter,
                integration_lock=None if handle.source_lock_held else self.source_lock,
            )
            await workspace.close()
            workspace = None
            if handle.source_lock_held:
                self.source_lock.release()
                handle.source_lock_held = False
            if isolated.accepted:
                if agent.changed:
                    invalidated_builds = await self._invalidate_build_records((handle.chapter.id,))
                    self._proof_rechecks.update(invalidated_builds)
                validation = ValidationResult(
                    True,
                    0,
                    "validation deferred to the topological fixup scheduler",
                )
            else:
                detail = isolated.error
                if isolated.out_of_scope_paths:
                    detail += ": " + ", ".join(isolated.out_of_scope_paths)
                agent = replace(agent, succeeded=False, error=detail)
                validation = ValidationResult(
                    False,
                    1,
                    f"Isolation rejected the agent result: {detail}",
                )
                await self.state.update_run(handle.run, status=TaskStatus.FAILED)
            await self.state.update_run(
                handle.run,
                isolation=isolated.as_dict(),
                validation=validation.as_dict(),
            )
        except BaseException as error:
            if workspace is not None:
                await workspace.close()
            if handle.source_lock_held:
                self.source_lock.release()
                handle.source_lock_held = False
            if handle.run.status == TaskStatus.RUNNING:
                detail = str(error) or type(error).__name__
                await self.state.finish_run(
                    handle.run,
                    status=TaskStatus.FAILED,
                    isolation={
                        "accepted": False,
                        "error": f"orchestration failed before integration: {detail}",
                    },
                )
            raise

        attempt = Attempt(agent=agent, validation=validation, run=handle.run)
        if agent.succeeded and validation.succeeded:
            await self.state.set_task(
                handle.chapter.id,
                Stage.FIXUP,
                TaskStatus.RUNNING,
                "fixup complete; queued for coordinator verification",
            )
            return attempt
        await self.state.set_task(
            handle.chapter.id,
            Stage.FIXUP,
            TaskStatus.FAILED,
            (
                "capacity retries exhausted; fixup failed"
                if agent.capacity_exhausted
                else "fixup agent failed"
            ),
        )
        return None

    async def _fixup_to_clean(
        self,
        feedback: dict[str, str] | None = None,
        *,
        target_ids: Iterable[str] | None = None,
        progress_event: asyncio.Event | None = None,
    ) -> bool:
        """Converge globally or until selected targets are clean in the observed DAG."""

        pending_feedback = dict(feedback or {})
        by_id = {chapter.id: chapter for chapter in self.chapters}
        targeted = target_ids is not None
        goals = set(target_ids or ())
        unknown_goals = goals.difference(by_id)
        if unknown_goals:
            raise ValueError(f"unknown fixup targets: {', '.join(sorted(unknown_goals))}")
        goals.update(chapter_id for chapter_id in pending_feedback if chapter_id in by_id)
        maximum = self.config.stages[Stage.FIXUP].max_rounds
        attempts = {chapter_id: 0 for chapter_id in by_id}
        running: dict[str, RunningFixupAgent] = {}
        failed: set[str] = set()
        persisted_clean = self.state.fixup_graph.get("clean", {})
        clean_records = persisted_clean if isinstance(persisted_clean, dict) else {}
        clean: dict[str, dict[str, Any]] = {}
        invalidated_clean: set[str] = set()
        build_generation = int(self.state.fixup_graph.get("build_generation", 0))

        def merge_feedback(items: dict[str, str]) -> None:
            for chapter_id, diagnostic in items.items():
                if chapter_id not in by_id:
                    continue
                if targeted:
                    goals.add(chapter_id)
                existing = pending_feedback.get(chapter_id, "")
                if diagnostic not in existing:
                    pending_feedback[chapter_id] = (
                        f"{existing}\n\n{diagnostic}" if existing else diagnostic
                    )

        async def fail_graph(error: ValueError) -> bool:
            self.state.fixup_graph = {"algorithm": "observed-lean-imports", "error": str(error)}
            async with self.state.batch():
                await self.state.save()
                await self.state.set_tasks(
                    by_id,
                    Stage.FIXUP,
                    TaskStatus.FAILED,
                    str(error),
                )
            return False

        async def cancel_running() -> None:
            handles = tuple(running.values())
            running.clear()
            for handle in handles:
                handle.task.cancel()
            await asyncio.gather(*(handle.task for handle in handles), return_exceptions=True)
            for handle in handles:
                await handle.workspace.close()
                if handle.source_lock_held:
                    self.source_lock.release()
                    handle.source_lock_held = False

        async def discard_stale(handle: RunningFixupAgent, changed: tuple[str, ...]) -> None:
            detail = "dependency changed while fixup agent was running: " + ", ".join(changed)
            await handle.workspace.close()
            if handle.source_lock_held:
                self.source_lock.release()
                handle.source_lock_held = False
            validation = ValidationResult(False, 1, detail)
            await self.state.update_run(
                handle.run,
                isolation={"accepted": False, "error": detail},
                validation=validation.as_dict(),
            )
            await self.state.set_task(
                handle.chapter.id,
                Stage.FIXUP,
                TaskStatus.RUNNING,
                "stale dependency snapshot; fixup requeued",
            )
            merge_feedback({handle.chapter.id: detail})

        async def build_chapter(
            chapter_id: str,
            graph: ChapterImportGraph,
            *,
            mode: str = "topological",
        ) -> bool:
            nonlocal build_generation, clean
            chapter = by_id[chapter_id]
            snapshots: dict[str, ValidatedBuildSnapshot] = {}
            result = (
                await self._build_chapters(
                    (chapter,),
                    publish_if_clean=True,
                    mode=mode,
                    iteration=min(attempts[chapter_id] + 1, maximum),
                    maximum_iterations=maximum,
                    stage=Stage.FIXUP,
                    priority=100.0,
                    snapshots=snapshots,
                )
            )[chapter_id]
            if result.succeeded:
                published = await self._publish_validated_build(chapter, snapshots[chapter_id])
                if not published:
                    merge_feedback(
                        {
                            chapter_id: (
                                "The source scope changed after its coordinator build; "
                                "rebuild the fresh generation."
                            )
                        }
                    )
                    return False
                persisted = self.state.fixup_graph.get("clean", {})
                records = persisted if isinstance(persisted, dict) else {}
                clean = self._retain_fixup_clean(self._observed_chapter_graph(), records)
                build_generation = int(self.state.fixup_graph.get("build_generation", 0))
                invalidated_clean.discard(chapter_id)
                await self.state.set_task(
                    chapter_id,
                    Stage.FIXUP,
                    TaskStatus.SUCCEEDED,
                    "clean coordinator build against observed imports",
                )
                if progress_event is not None:
                    progress_event.set()
                return True

            diagnostics = self._build_feedback({chapter_id: result}).actionable
            merge_feedback(diagnostics)
            invalidated_clean.update(
                self._invalidate_fixup_descendants(
                    graph,
                    clean,
                    diagnostics or (chapter_id,),
                )
            )
            await self._save_fixup_graph(
                graph,
                clean,
                build_generation=build_generation,
                invalidated=invalidated_clean,
            )
            return False

        async def start_actionable_fixups(graph: ChapterImportGraph) -> None:
            """Fill free agent slots from the dependency-ready feedback frontier."""

            active = sum(not handle.task.done() for handle in running.values())
            available = self.config.settings.max_agents - active
            if self.isolation.name == "shared":
                available = min(available, 1 - active)
            actionable = [
                chapter_id
                for chapter_id in graph.order
                if chapter_id in pending_feedback
                and chapter_id not in running
                and chapter_id not in failed
                and graph.dependencies[chapter_id].issubset(clean)
            ]
            for chapter_id in actionable[: max(available, 0)]:
                if self.state.task(chapter_id, Stage.REVIEW).status in {
                    TaskStatus.RUNNING,
                    TaskStatus.SUCCEEDED,
                }:
                    await self._invalidate_reviews(
                        (chapter_id,),
                        detail="review invalidated by later fixup findings",
                    )
                    if progress_event is not None:
                        progress_event.set()
                if attempts[chapter_id] >= maximum:
                    await self.state.set_task(
                        chapter_id,
                        Stage.FIXUP,
                        TaskStatus.FAILED,
                        f"fixup did not converge in {maximum} attempts",
                    )
                    pending_feedback.pop(chapter_id, None)
                    failed.add(chapter_id)
                    invalidated_clean.update(
                        self._invalidate_fixup_descendants(graph, clean, (chapter_id,))
                    )
                    continue
                attempts[chapter_id] += 1
                dependency_certificates = {
                    dependency: str(clean[dependency]["certificate"])
                    for dependency in graph.dependencies[chapter_id]
                }
                running[chapter_id] = await self._start_fixup_agent(
                    by_id[chapter_id],
                    pending_feedback.pop(chapter_id),
                    dependency_certificates,
                )

        try:
            graph = self._observed_chapter_graph()
        except ValueError as error:
            return await fail_graph(error)

        clean = self._retain_fixup_clean(graph, clean_records)
        invalidated_clean.update(self._invalidate_fixup_descendants(graph, clean, pending_feedback))
        await self._save_fixup_graph(
            graph,
            clean,
            build_generation=build_generation,
            invalidated=invalidated_clean,
        )
        reusable = set(clean).intersection(goals if targeted else by_id)
        if reusable:
            await self.state.set_tasks(
                reusable,
                Stage.FIXUP,
                TaskStatus.SUCCEEDED,
                "clean initial build reused",
            )
            if progress_event is not None:
                progress_event.set()

        if targeted and goals.issubset(clean) and not pending_feedback:
            return True

        # Drafting deliberately skips Lean validation. Before spending an
        # agent slot on any resulting fixup feedback, optimistically build the
        # complete selected closure once. Stream both successful chapters and
        # dependency-ready diagnostics to their consumers as each build ends.
        optimistic_ids = self._dependency_closure(graph, goals) if targeted else set(by_id)
        for chapter_id in graph.order:
            if chapter_id not in optimistic_ids:
                continue
            if await build_chapter(chapter_id, graph, mode="optimistic"):
                pending_feedback.pop(chapter_id, None)
            if self.isolation.name != "shared":
                await start_actionable_fixups(graph)

        # A successful target can certify imported predecessors too. Reconcile
        # all records after the pass before deciding whether any feedback still
        # warrants an agent.
        graph = self._observed_chapter_graph()
        persisted = self.state.fixup_graph.get("clean", {})
        clean = self._retain_fixup_clean(
            graph,
            persisted if isinstance(persisted, dict) else {},
        )
        optimistic_clean = optimistic_ids.intersection(clean)
        for chapter_id in optimistic_clean:
            pending_feedback.pop(chapter_id, None)
        invalidated_clean.difference_update(optimistic_clean)
        if optimistic_clean:
            await self.state.set_tasks(
                optimistic_clean,
                Stage.FIXUP,
                TaskStatus.SUCCEEDED,
                "clean optimistic coordinator build; no fixup agent needed",
            )
            if progress_event is not None:
                progress_event.set()
        if not pending_feedback and (
            (targeted and goals.issubset(clean)) or (not targeted and len(clean) == len(by_id))
        ):
            return True

        try:
            while True:
                await self.control.checkpoint()
                try:
                    rescanned = self._observed_chapter_graph()
                except ValueError as error:
                    return await fail_graph(error)

                graph_changed = rescanned.edges != graph.edges
                graph = rescanned
                clean = self._retain_fixup_clean(graph, clean)
                await self._save_fixup_graph(
                    graph,
                    clean,
                    build_generation=build_generation,
                    invalidated=invalidated_clean,
                )

                if targeted and goals.issubset(clean) and not pending_feedback and not running:
                    await self.state.set_tasks(
                        goals,
                        Stage.FIXUP,
                        TaskStatus.SUCCEEDED,
                        "clean initial build reused",
                    )
                    return True

                completed = [
                    chapter_id for chapter_id, handle in running.items() if handle.task.done()
                ]
                if completed:
                    order = {chapter_id: index for index, chapter_id in enumerate(graph.order)}
                    chapter_id = min(completed, key=lambda item: order.get(item, len(order)))
                    handle = running.pop(chapter_id)
                    try:
                        await handle.task
                    except BaseException:
                        await handle.workspace.close()
                        raise
                    changed_dependencies = tuple(
                        dependency
                        for dependency, certificate in handle.dependency_certificates.items()
                        if dependency not in clean
                        or clean[dependency].get("certificate") != certificate
                    )
                    if changed_dependencies:
                        await discard_stale(handle, changed_dependencies)
                        continue
                    attempt = await self._integrate_fixup_agent(handle)
                    if progress_event is not None:
                        progress_event.set()
                    if attempt is None:
                        if attempts[chapter_id] < maximum:
                            merge_feedback(
                                {
                                    chapter_id: (
                                        "The previous fixup agent failed or exhausted its capacity "
                                        "budget before producing an acceptable patch. Retry the "
                                        "scoped repair from the coordinator diagnostics."
                                    )
                                }
                            )
                        else:
                            failed.add(chapter_id)
                            invalidated_clean.update(
                                self._invalidate_fixup_descendants(graph, clean, (chapter_id,))
                            )
                        continue
                    clean.pop(chapter_id, None)
                    try:
                        graph = self._observed_chapter_graph()
                    except ValueError as error:
                        return await fail_graph(error)
                    clean = self._retain_fixup_clean(graph, clean)
                    if graph.dependencies[chapter_id].issubset(clean):
                        await build_chapter(chapter_id, graph)
                    continue

                if (
                    not targeted
                    and len(clean) == len(by_id)
                    and not pending_feedback
                    and not running
                ):
                    ordered = tuple(by_id[chapter_id] for chapter_id in graph.order)
                    snapshots: dict[str, ValidatedBuildSnapshot] = {}
                    results = await self._build_chapters(
                        ordered,
                        publish_if_clean=True,
                        mode="stable-topological",
                        iteration=1,
                        maximum_iterations=1,
                        snapshots=snapshots,
                    )
                    verified = self._observed_chapter_graph()
                    clean = self._retain_fixup_clean(verified, clean)
                    if (
                        all(result.succeeded for result in results.values())
                        and verified.edges == graph.edges
                        and len(clean) == len(by_id)
                        and await self._publish_validated_builds(snapshots)
                    ):
                        build_generation = int(self.state.fixup_graph.get("build_generation", 0))
                        persisted = self.state.fixup_graph.get("clean", {})
                        clean = self._retain_fixup_clean(
                            verified,
                            persisted if isinstance(persisted, dict) else {},
                        )
                        invalidated_clean.clear()
                        await self.state.set_tasks(
                            by_id,
                            Stage.FIXUP,
                            TaskStatus.SUCCEEDED,
                            "stable clean build in observed import order",
                        )
                        return True
                    graph = verified
                    diagnostics = self._build_feedback(results).actionable
                    merge_feedback(diagnostics)
                    invalidated_clean.update(
                        self._invalidate_fixup_descendants(graph, clean, diagnostics)
                    )
                    if not diagnostics and not graph_changed:
                        break
                    continue

                await start_actionable_fixups(graph)

                if self.isolation.name == "shared" and running:
                    await asyncio.wait(
                        (handle.task for handle in running.values()),
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    continue

                needed = self._dependency_closure(graph, set(goals) | set(pending_feedback))
                buildable = [
                    chapter_id
                    for chapter_id in graph.order
                    if chapter_id not in clean
                    and chapter_id not in pending_feedback
                    and chapter_id not in running
                    and chapter_id not in failed
                    and graph.dependencies[chapter_id].issubset(clean)
                    and (not targeted or chapter_id in needed)
                ]
                if buildable:
                    await build_chapter(buildable[0], graph)
                    continue

                if running:
                    await asyncio.wait(
                        (handle.task for handle in running.values()),
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    continue
                break
        finally:
            await cancel_running()

        required = self._dependency_closure(graph, set(goals) if targeted else set(by_id))
        unresolved = required.difference(clean)
        blocked = unresolved.difference(failed)
        if blocked:
            await self.state.set_tasks(
                blocked,
                Stage.FIXUP,
                TaskStatus.BLOCKED if failed else TaskStatus.FAILED,
                (
                    "blocked by a failed prerequisite fixup; unrelated branches completed"
                    if failed
                    else "observed-import fixup could not select dependency-ready work"
                ),
            )
        return False

    async def _formalize_all(self) -> bool:
        """Run every formalizer once and retain chapter-local failures."""

        running = {
            asyncio.create_task(self._formalize(chapter)): chapter for chapter in self.chapters
        }
        had_failure = False
        try:
            while running:
                done, _ = await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    running.pop(task)
                    outcome = task.result()
                    if not outcome.succeeded:
                        had_failure = True
        except BaseException:
            for task in running:
                task.cancel()
            await asyncio.gather(*running, return_exceptions=True)
            raise
        return not had_failure

    async def _review_once(
        self,
        chapter: Chapter,
        *,
        rerun: bool = False,
        feedback: str = "",
    ) -> ReviewOutcome:
        if not rerun and self._already_done(chapter, Stage.REVIEW):
            return ReviewOutcome(True, False, {}, complete=True)
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
                {},
                complete=False,
                run_id=attempt.run.id,
            )
        succeeded = attempt.agent.succeeded and attempt.validation.succeeded
        complete = bool(attempt.agent.report.get("complete"))
        routed = self._route_review_findings(chapter, attempt.agent.report)
        if succeeded and complete and not routed:
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
                {},
                complete=True,
                run_id=attempt.run.id,
            )
        detail = "review left follow-up findings" if routed else "editing review failed"
        await self.state.set_task(
            chapter.id,
            Stage.REVIEW,
            TaskStatus.FAILED,
            detail,
        )
        return ReviewOutcome(
            succeeded,
            attempt.agent.changed,
            routed,
            complete=complete,
            run_id=attempt.run.id,
        )

    def _review_invalidation_generation(self, chapter_id: str) -> int:
        return self._review_invalidation_generations.get(chapter_id, 0)

    async def _complete_review(
        self,
        chapter: Chapter,
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
        quiesce_downstream: bool = False,
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
            if quiesce_downstream:
                self._quiescent_review_rebuilds.update(targets)
            await self.state.set_tasks(
                targets,
                Stage.REVIEW,
                TaskStatus.PENDING,
                detail,
            )
        return targets

    async def _review_build(self, chapter: Chapter) -> dict[str, str]:
        """Build review output once, returning diagnostics to review rather than fixup."""

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
        """Persist follow-up work and reopen only its owners after descendants drain."""

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
            quiesce_downstream=True,
        )
        return request_id, invalidated

    async def _review_chapter_to_clean(
        self,
        chapter: Chapter,
        rounds_used: dict[str, int],
        *,
        rerun: bool = False,
        feedback: str = "",
        proof_request_ids: tuple[str, ...] = (),
    ) -> bool:
        """Run at most five edit/rebuild cycles for one dependency-ready chapter."""

        review_generation = self._review_invalidation_generation(chapter.id)
        if self.state.task(chapter.id, Stage.REVIEW).status == TaskStatus.SUCCEEDED:
            return True
        graph = self._observed_chapter_graph()
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
            external = set(items).difference({chapter.id})
            if chapter.id in self._successor_closure(graph, external) if external else False:
                await self.state.set_task(
                    chapter.id,
                    Stage.REVIEW,
                    TaskStatus.PENDING,
                    "waiting for prerequisite follow-up reviews",
                )
                return False
            if chapter.id in items:
                await self.state.set_task(
                    chapter.id,
                    Stage.REVIEW,
                    TaskStatus.RUNNING,
                    "review follow-up queued",
                )
            return True

        persisted = self.state.fixup_graph.get("clean", {})
        records = persisted if isinstance(persisted, dict) else {}
        was_clean = chapter.id in self._retain_fixup_clean(graph, records)
        if not was_clean:
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
            findings_by_owner: dict[str, dict[str, None]] = {}
            for owner, feedback in outcome.feedback_by_owner.items():
                findings_by_owner.setdefault(owner, {})[feedback] = None
            findings = {owner: "\n\n".join(blocks) for owner, blocks in findings_by_owner.items()}
            if findings and not await route_feedback(
                findings,
                origin=f"review:{outcome.run_id or uuid4().hex[:12]}",
            ):
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
                if not outcome.changed and not findings and not build_feedback:
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
        fixup: RunningFixupStage | None = None,
    ) -> bool:
        """Release dependency-ready reviews and proofs without a corpus-wide fixup gate."""

        await self._recover_proof_review_requests()
        by_id = {chapter.id: chapter for chapter in self.chapters}
        quarantined_ids = set(quarantined).intersection(by_id)
        try:
            initial_graph = self._observed_chapter_graph()
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
            for chapter in self.chapters
            if chapter.id not in quarantined_ids
            and self.state.task(chapter.id, Stage.REVIEW).status == TaskStatus.SUCCEEDED
        }
        review_failures: set[str] = set(quarantined_ids)
        review_blocked: set[str] = set()
        attempted: set[str] = set()
        rounds_used = {chapter_id: 0 for chapter_id in by_id}
        review_tasks: dict[str, RunningReview] = {}
        proof_tasks: dict[str, asyncio.Task[bool]] = {}
        persisted_clean = self.state.fixup_graph.get("clean", {})
        clean = self._retain_fixup_clean(
            initial_graph,
            persisted_clean if isinstance(persisted_clean, dict) else {},
        )
        proof_results = {
            chapter_id: True
            for chapter_id in reviewed
            if (
                initial_graph.dependencies[chapter_id].issubset(reviewed)
                and self.state.task(chapter_id, Stage.PROVE).status == TaskStatus.SUCCEEDED
                and isinstance(clean.get(chapter_id), dict)
                and self.state.task(chapter_id, Stage.PROVE).source_digest
                == clean[chapter_id].get("source_digest")
            )
        }
        proof_reviews = {chapter_id: 0 for chapter_id in by_id}
        fixup_failures_applied = False

        def fixup_ready(chapter_id: str) -> bool:
            return (
                fixup is None
                or chapter_id not in fixup.target_ids
                or self.state.task(chapter_id, Stage.FIXUP).status == TaskStatus.SUCCEEDED
            )

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
                    if not fixup_ready(chapter_id):
                        await self.state.set_task(
                            chapter_id,
                            Stage.REVIEW,
                            TaskStatus.PENDING,
                            "waiting for clean initial fixup",
                        )
                    else:
                        missing = initial_graph.dependencies[chapter_id].difference(reviewed)
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
            tasks = [handle.task for handle in review_tasks.values()] + list(proof_tasks.values())
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        try:
            while True:
                if fixup is not None:
                    # Clear before inspecting state so a subsequent fixup
                    # transition cannot be lost between the scan and wait.
                    fixup.progress.clear()
                try:
                    graph = self._observed_chapter_graph()
                except ValueError as error:
                    await self.state.set_tasks(
                        by_id,
                        Stage.REVIEW,
                        TaskStatus.FAILED,
                        str(error),
                    )
                    return False

                if fixup is not None and fixup.task.done() and not fixup_failures_applied:
                    # Propagate orchestration failures, then quarantine only
                    # chapters whose own fixup did not succeed. Independent
                    # clean branches remain eligible for review and proof.
                    fixup.task.result()
                    failed_fixups = {
                        chapter_id
                        for chapter_id in fixup.target_ids
                        if self.state.task(chapter_id, Stage.FIXUP).status != TaskStatus.SUCCEEDED
                    }
                    if failed_fixups:
                        reviewed.difference_update(failed_fixups)
                        review_failures.update(failed_fixups)
                        cancelled_proofs = [
                            proof_tasks.pop(chapter_id)
                            for chapter_id in failed_fixups
                            if chapter_id in proof_tasks
                        ]
                        for task in cancelled_proofs:
                            task.cancel()
                        await asyncio.gather(*cancelled_proofs, return_exceptions=True)
                        for chapter_id in failed_fixups:
                            proof_results.pop(chapter_id, None)
                        await self._invalidate_reviews(
                            failed_fixups,
                            detail="review blocked by failed initial fixup",
                        )
                        async with self.state.batch():
                            await self.state.set_tasks(
                                failed_fixups,
                                Stage.REVIEW,
                                TaskStatus.FAILED,
                                "initial fixup did not complete",
                            )
                            if prove:
                                await self.state.set_tasks(
                                    failed_fixups,
                                    Stage.PROVE,
                                    TaskStatus.BLOCKED,
                                    "blocked because initial fixup did not complete",
                                )
                    fixup_failures_applied = True

                # Pull durable successes into readiness and remove reviews with
                # direct findings. Source edits separately trigger build rechecks.
                reviewed.difference_update(self._invalidated_reviews)
                self._invalidated_reviews.clear()
                pending_rechecks = set(self._proof_rechecks)
                self._proof_rechecks.clear()
                for chapter_id in pending_rechecks:
                    proof_results.pop(chapter_id, None)
                    if chapter_id in proof_tasks:
                        self._proof_rechecks.add(chapter_id)
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

                active_chapters = review_tasks.keys() | proof_tasks.keys()
                for chapter_id in graph.order:
                    waiting_for_descendants = (
                        chapter_id in self._quiescent_review_rebuilds
                        and bool(
                            self._successor_closure(graph, (chapter_id,)).difference({chapter_id})
                            & active_chapters
                        )
                    )
                    if (
                        chapter_id not in reviewed
                        and chapter_id not in review_failures
                        and chapter_id not in review_blocked
                        and chapter_id not in review_tasks
                        and fixup_ready(chapter_id)
                        and graph.dependencies[chapter_id].issubset(reviewed)
                        and not waiting_for_descendants
                    ):
                        dependencies = graph.dependencies[chapter_id]
                        proof_feedback, proof_request_ids = self._proof_review_feedback(chapter_id)
                        await self.state.set_task(
                            chapter_id,
                            Stage.REVIEW,
                            TaskStatus.RUNNING,
                            "waiting for dependency-ordered coordinator build",
                        )
                        review_operation = (
                            self._review_chapter_to_clean(
                                by_id[chapter_id],
                                rounds_used,
                                rerun=rerun or chapter_id in attempted,
                                feedback=proof_feedback,
                                proof_request_ids=proof_request_ids,
                            )
                            if proof_feedback
                            else self._review_chapter_to_clean(
                                by_id[chapter_id],
                                rounds_used,
                                rerun=rerun or chapter_id in attempted,
                            )
                        )
                        review_tasks[chapter_id] = RunningReview(
                            task=asyncio.create_task(review_operation),
                            dependencies=dependencies,
                            proof_request_ids=proof_request_ids,
                        )
                        self._quiescent_review_rebuilds.discard(chapter_id)
                        attempted.add(chapter_id)

                if prove:
                    for chapter_id in reviewed:
                        if (
                            chapter_id not in proof_results
                            and chapter_id not in proof_tasks
                            and fixup_ready(chapter_id)
                            and graph.dependencies[chapter_id].issubset(reviewed)
                        ):
                            proof_tasks[chapter_id] = asyncio.create_task(
                                self._prove(by_id[chapter_id], defer_review=True)
                            )

                live_tasks = [handle.task for handle in review_tasks.values()]
                live_tasks.extend(proof_tasks.values())
                progress_waiter: asyncio.Task[bool] | None = None
                if fixup is not None and not fixup.task.done():
                    progress_waiter = asyncio.create_task(fixup.progress.wait())
                    live_tasks.extend((fixup.task, progress_waiter))
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
                    current_graph = self._observed_chapter_graph()
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
                    if (
                        prove
                        and chapter_id not in proof_results
                        and chapter_id not in proof_tasks
                        and current_dependencies.issubset(reviewed)
                    ):
                        proof_tasks[chapter_id] = asyncio.create_task(
                            self._prove(by_id[chapter_id], defer_review=True)
                        )

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
                    report = proof_task.runs[-1].report if proof_task.runs else None
                    if not isinstance(report, dict) or not report.get("fixup_findings"):
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
                    proof_run = proof_task.runs[-1]
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
        chapter: Chapter,
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

    async def _prove(self, chapter: Chapter, *, defer_review: bool = False) -> bool:
        initial_feedback = ""
        if not self.force:
            graph = self._observed_chapter_graph()
            persisted = self.state.fixup_graph.get("clean", {})
            clean = self._retain_fixup_clean(
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
        while proof_round < proof_maximum:
            attempt = await self._attempt(
                chapter,
                Stage.PROVE,
                feedback=feedback,
                queue_detail=f"proof round {proof_round + 1}/{proof_maximum}",
            )
            if attempt.agent.capacity_exhausted:
                await self.state.set_task(
                    chapter.id,
                    Stage.PROVE,
                    TaskStatus.FAILED,
                    "model capacity remained unavailable after the configured retries",
                )
                return False
            proof_round += 1
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
            if bool(attempt.agent.report.get("fixup_findings")):
                await self.state.set_task(
                    chapter.id,
                    Stage.PROVE,
                    TaskStatus.PENDING,
                    "proof reported a possible statement issue; waiting for targeted review",
                )
                if not defer_review:
                    await self._queue_proof_review(
                        chapter,
                        attempt.agent.report,
                        origin_run_id=attempt.run.id,
                    )
                return False
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
        if stage is Stage.FIXUP:
            return await self._fixup_to_clean()
        if stage is Stage.REVIEW:
            return await self._review_until_clean()
        if stage is Stage.FORMALIZE:
            return await self._formalize_all()
        return all(await _gather_cancel_on_error(self._prove(chapter) for chapter in self.chapters))

    async def run_pipeline(self) -> bool:
        formalized = await self._formalize_all()
        quarantined = (
            set()
            if formalized
            else {
                chapter.id
                for chapter in self.chapters
                if self.state.task(chapter.id, Stage.FORMALIZE).status != TaskStatus.SUCCEEDED
            }
        )
        initial_targets = {chapter.id for chapter in self.chapters}.difference(quarantined)
        if not initial_targets:
            return False
        fixup_progress = asyncio.Event()
        fixup_task = asyncio.create_task(
            self._fixup_to_clean(
                target_ids=initial_targets,
                progress_event=fixup_progress,
            )
        )
        fixup = RunningFixupStage(
            task=fixup_task,
            progress=fixup_progress,
            target_ids=frozenset(initial_targets),
        )
        review_task = asyncio.create_task(
            self._review_tree(
                prove=True,
                quarantined=quarantined,
                fixup=fixup,
            )
        )
        try:
            fixed, reviewed = await asyncio.gather(fixup_task, review_task)
        except BaseException:
            fixup_task.cancel()
            review_task.cancel()
            await asyncio.gather(fixup_task, review_task, return_exceptions=True)
            raise
        return formalized and fixed and reviewed
