from __future__ import annotations

import asyncio
import hashlib
import heapq
import re
from collections.abc import AsyncIterator, Coroutine, Iterable
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from typing import Any

from lastlib_swarm.codex import (
    AgentResult,
    CodexExecutor,
    ValidationResult,
    scope_digest,
    validate,
)
from lastlib_swarm.corpus import (
    ChapterImportGraph,
    build_chapter_import_graph,
    build_corpus_schedule,
    scheduling_snapshot,
)
from lastlib_swarm.diagnostics import unexpected_lean_warnings
from lastlib_swarm.isolation import create_isolation
from lastlib_swarm.models import Chapter, PipelineConfig, Stage
from lastlib_swarm.state import RunRecord, StateStore, TaskPhase, TaskStatus


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
    capacity_deferred: bool = False


@dataclass(frozen=True)
class ReviewOutcome:
    succeeded: bool
    changed: bool
    feedback_by_owner: dict[str, str]
    complete: bool = True


@dataclass(frozen=True)
class BuildDiagnostics:
    actionable: dict[str, str]
    deferred_owner_ids: tuple[str, ...]


@dataclass
class RunningFixupAgent:
    chapter: Chapter
    run: RunRecord
    workspace: Any
    task: asyncio.Task[AgentResult]
    dependency_certificates: dict[str, str]


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


class PriorityLimiter:
    """A concurrency limiter that grants slots to the highest-priority waiter."""

    def __init__(self, capacity: int) -> None:
        self.capacity = capacity
        self.available = capacity
        self._sequence = 0
        self._waiters: list[tuple[float, int, asyncio.Future[None]]] = []

    def _wake(self) -> None:
        while self.available and self._waiters:
            _, _, waiter = heapq.heappop(self._waiters)
            if waiter.done():
                continue
            self.available -= 1
            waiter.set_result(None)

    async def acquire(self, priority: float) -> None:
        loop = asyncio.get_running_loop()
        waiter: asyncio.Future[None] = loop.create_future()
        self._sequence += 1
        heapq.heappush(self._waiters, (-priority, self._sequence, waiter))
        self._wake()
        try:
            await waiter
        except asyncio.CancelledError:
            # Cancellation can race with a grant. Return a granted slot; otherwise
            # leave a cancelled future for _wake to discard lazily.
            if waiter.done() and not waiter.cancelled():
                self.release()
            else:
                waiter.cancel()
            raise

    def release(self) -> None:
        if self.available >= self.capacity:
            raise RuntimeError("priority limiter released without an acquired slot")
        self.available += 1
        self._wake()

    @asynccontextmanager
    async def slot(self, priority: float) -> AsyncIterator[None]:
        await self.acquire(priority)
        try:
            yield
        finally:
            self.release()


class CoordinatorBuildQueue:
    """Fair gate for source integration and main-worktree builds."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        await self._lock.acquire()

    def release(self) -> None:
        self._lock.release()


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
                "coordinator-owned-main-cache-with-read-only-snapshots"
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
        self.fixup_lock = asyncio.Lock()

    def scheduling_snapshot(self) -> dict[str, object]:
        return scheduling_snapshot(self.statement_schedule, self.proof_schedule)

    async def prepare(self) -> None:
        await self.state.load_or_create()
        await self.executor.prepare()
        self.scaffold()
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
    def _invalidate_fixup_descendants(
        graph: ChapterImportGraph,
        clean: dict[str, dict[str, Any]],
        chapter_ids: Iterable[str],
    ) -> None:
        pending = list(chapter_ids)
        invalidated: set[str] = set()
        while pending:
            chapter_id = pending.pop()
            if chapter_id in invalidated:
                continue
            invalidated.add(chapter_id)
            pending.extend(graph.successors.get(chapter_id, ()))
        for chapter_id in invalidated:
            clean.pop(chapter_id, None)

    async def _save_fixup_graph(
        self,
        graph: ChapterImportGraph,
        clean: dict[str, dict[str, Any]],
        *,
        build_generation: int,
    ) -> int:
        previous = self.state.fixup_graph
        previous_edges = previous.get("edges") if isinstance(previous, dict) else None
        edges = [list(edge) for edge in graph.edges]
        revision = int(previous.get("revision", 0)) if isinstance(previous, dict) else 0
        if previous.get("algorithm") != "observed-lean-imports" or previous_edges != edges:
            revision += 1
        self.state.fixup_graph = graph.snapshot() | {
            "revision": revision,
            "build_generation": build_generation,
            "clean": clean,
        }
        await self.state.save()
        return revision

    def _scope_exists(self, chapter: Chapter) -> bool:
        repo = self.config.settings.repo
        return all(any(path.is_file() for path in repo.glob(pattern)) for pattern in chapter.scope)

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
            phase=TaskPhase.QUEUED,
        )
        await self.control.checkpoint()
        schedule = (
            self.statement_schedule
            if stage in (Stage.FORMALIZE, Stage.FIXUP, Stage.REVIEW)
            else self.proof_schedule
        )
        async with self.agent_slots.slot(schedule.priority(chapter.book_id)):
            await self.control.checkpoint()
            run = await self.state.start_run(chapter.id, stage)
            workspace = None
            queue_held = False
            try:
                # Snapshot acquisition shares the build gate. A new overlay can
                # therefore never observe merged sources paired with a stale cache.
                await self.build_queue.acquire()
                queue_held = True
                try:
                    workspace = await self.isolation.acquire(run.id)
                finally:
                    # Shared worktrees cannot isolate edits from builds, so they
                    # serialize the whole attempt. Overlay agents release the gate
                    # immediately and remain concurrent while they edit.
                    if self.isolation.name != "shared":
                        self.build_queue.release()
                        queue_held = False
                agent = await self.executor.run(
                    chapter,
                    stage,
                    run,
                    feedback=feedback,
                    workspace_root=workspace.root,
                )
                if not queue_held:
                    await self.build_queue.acquire()
                    queue_held = True
                # The agent is finished before this transaction begins. Merge its
                # scoped source changes, unmount its workspace, and only then build
                # from the main worktree against the coordinator-owned cache.
                isolated = await workspace.collect(chapter)
                await workspace.close()
                workspace = None
                if isolated.accepted:
                    if stage is Stage.PROVE:
                        validation = await validate(
                            self.config,
                            chapter,
                            workspace_root=self.config.settings.repo,
                        )
                        await self.isolation.refresh_cache()
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
                if run.status == TaskStatus.RUNNING:
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
                if workspace is not None:
                    await workspace.close()
                if queue_held:
                    self.build_queue.release()
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
        if attempt.agent.succeeded and attempt.validation.succeeded:
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
                TaskStatus.PENDING,
                "capacity retries exhausted; requeued behind waiting formalizers",
            )
            return FormalizeOutcome(False, capacity_deferred=True)
        await self.state.set_task(
            chapter.id,
            Stage.FORMALIZE,
            TaskStatus.FAILED,
            "single drafting attempt failed",
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
    ) -> dict[str, ValidationResult]:
        """Build a deterministic target batch against the coordinator-owned cache."""

        selected = tuple(chapters)
        results: dict[str, ValidationResult] = {}
        await self.build_queue.acquire()
        try:
            if selected:
                async with self.state.batch():
                    await self.state.set_tasks(
                        (chapter.id for chapter in selected),
                        Stage.FIXUP,
                        TaskStatus.RUNNING,
                        f"{mode} coordinator build {iteration}/{maximum_iterations}",
                        phase=TaskPhase.BUILDING,
                    )
                    await self.state.start_coordinator_build(
                        mode=mode,
                        stage=Stage.FIXUP,
                        iteration=iteration,
                        maximum_iterations=maximum_iterations,
                        total=len(selected),
                    )
            for index, chapter in enumerate(selected):
                await self.control.checkpoint()
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

                results[chapter.id] = await validate(
                    self.config,
                    chapter,
                    workspace_root=self.config.settings.repo,
                    on_output=append_output,
                )
                await self.state.advance_coordinator_build(
                    chapter_id=chapter.id,
                    completed=index + 1,
                )
            if (
                publish_if_clean
                and results
                and all(result.succeeded for result in results.values())
            ):
                await self.isolation.refresh_cache()
        finally:
            try:
                if selected:
                    async with self.state.batch():
                        await self.state.finish_coordinator_build()
                        await self.state.set_tasks(
                            (chapter.id for chapter in selected),
                            Stage.FIXUP,
                            TaskStatus.RUNNING,
                            "awaiting coordinator rebuild",
                            phase=TaskPhase.AWAITING_REBUILD,
                        )
            finally:
                self.build_queue.release()
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
            f"fixup attempt {task.rounds + 1} (global cap {maximum})",
            phase=TaskPhase.QUEUED,
        )
        await self.control.checkpoint()
        await self.agent_slots.acquire(self.statement_schedule.priority(chapter.book_id))
        run = None
        workspace = None
        try:
            run = await self.state.start_run(chapter.id, Stage.FIXUP)
            await self.build_queue.acquire()
            try:
                workspace = await self.isolation.acquire(run.id)
            finally:
                self.build_queue.release()

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
            )
        except BaseException as error:
            self.agent_slots.release()
            if workspace is not None:
                await workspace.close()
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
            await self.build_queue.acquire()
            try:
                isolated = await workspace.collect(handle.chapter)
                await workspace.close()
                workspace = None
            finally:
                self.build_queue.release()
            if isolated.accepted:
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
                "fixup complete; awaiting coordinator rebuild",
                phase=TaskPhase.AWAITING_REBUILD,
            )
            return attempt
        await self.state.set_task(
            handle.chapter.id,
            Stage.FIXUP,
            TaskStatus.FAILED,
            "fixup agent failed",
        )
        return None

    async def _fixup_to_clean(
        self,
        feedback: dict[str, str] | None = None,
        *,
        target_ids: Iterable[str] | None = None,
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

        async def discard_stale(handle: RunningFixupAgent, changed: tuple[str, ...]) -> None:
            detail = "dependency changed while fixup agent was running: " + ", ".join(changed)
            await handle.workspace.close()
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
                phase=TaskPhase.QUEUED,
            )
            merge_feedback({handle.chapter.id: detail})

        async def build_chapter(chapter_id: str, graph: ChapterImportGraph) -> bool:
            nonlocal build_generation
            chapter = by_id[chapter_id]
            result = (
                await self._build_chapters(
                    (chapter,),
                    publish_if_clean=True,
                    mode="topological",
                    iteration=self.state.task(chapter_id, Stage.FIXUP).rounds + 1,
                    maximum_iterations=maximum,
                )
            )[chapter_id]
            if result.succeeded:
                source = scope_digest(self.config.settings.repo, chapter)
                certificate = self._fixup_certificate(
                    source,
                    graph.dependencies[chapter_id],
                    clean,
                )
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
                await self.state.set_task(
                    chapter_id,
                    Stage.FIXUP,
                    TaskStatus.SUCCEEDED,
                    "clean coordinator build against observed imports",
                )
                return True

            diagnostics = self._build_feedback({chapter_id: result}).actionable
            merge_feedback(diagnostics)
            self._invalidate_fixup_descendants(
                graph,
                clean,
                diagnostics or (chapter_id,),
            )
            return False

        try:
            graph = self._observed_chapter_graph()
        except ValueError as error:
            return await fail_graph(error)

        clean = self._retain_fixup_clean(graph, clean_records)
        self._invalidate_fixup_descendants(graph, clean, pending_feedback)
        await self._save_fixup_graph(
            graph,
            clean,
            build_generation=build_generation,
        )

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
                )

                if targeted and goals.issubset(clean) and not pending_feedback and not running:
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
                    if attempt is None:
                        if attempts[chapter_id] < maximum:
                            merge_feedback(
                                {
                                    chapter_id: (
                                        "The previous fixup agent failed before producing an "
                                        "acceptable patch. Retry the scoped repair from the "
                                        "coordinator diagnostics."
                                    )
                                }
                            )
                        else:
                            failed.add(chapter_id)
                            self._invalidate_fixup_descendants(graph, clean, (chapter_id,))
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
                    results = await self._build_chapters(
                        ordered,
                        publish_if_clean=True,
                        mode="stable-topological",
                        iteration=1,
                        maximum_iterations=1,
                    )
                    verified = self._observed_chapter_graph()
                    clean = self._retain_fixup_clean(verified, clean)
                    if (
                        all(result.succeeded for result in results.values())
                        and verified.edges == graph.edges
                        and len(clean) == len(by_id)
                    ):
                        build_generation += 1
                        for record in clean.values():
                            record["build_generation"] = build_generation
                        await self._save_fixup_graph(
                            verified,
                            clean,
                            build_generation=build_generation,
                        )
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
                    self._invalidate_fixup_descendants(graph, clean, diagnostics)
                    if not diagnostics and not graph_changed:
                        break
                    continue

                available = self.config.settings.max_agents - len(running)
                if self.isolation.name == "shared":
                    available = min(available, 1 - len(running))
                actionable = [
                    chapter_id
                    for chapter_id in graph.order
                    if chapter_id in pending_feedback
                    and chapter_id not in running
                    and chapter_id not in failed
                    and graph.dependencies[chapter_id].issubset(clean)
                ]
                for chapter_id in actionable[: max(available, 0)]:
                    if attempts[chapter_id] >= maximum:
                        await self.state.set_task(
                            chapter_id,
                            Stage.FIXUP,
                            TaskStatus.FAILED,
                            f"fixup did not converge in {maximum} attempts",
                        )
                        pending_feedback.pop(chapter_id, None)
                        failed.add(chapter_id)
                        self._invalidate_fixup_descendants(graph, clean, (chapter_id,))
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

                if self.isolation.name == "shared" and running:
                    await asyncio.wait(
                        (handle.task for handle in running.values()),
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    continue

                needed = set(goals) | set(pending_feedback)
                pending_needed = list(needed)
                while pending_needed:
                    required_by = pending_needed.pop()
                    for dependency in graph.dependencies.get(required_by, frozenset()):
                        if dependency not in needed:
                            needed.add(dependency)
                            pending_needed.append(dependency)
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

        required = set(goals) if targeted else set(by_id)
        pending_required = list(required)
        while pending_required:
            chapter_id = pending_required.pop()
            for dependency in graph.dependencies.get(chapter_id, frozenset()):
                if dependency not in required:
                    required.add(dependency)
                    pending_required.append(dependency)
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
        """Run every formalizer, requeueing capacity-deferred chapters at the back."""

        running = {
            asyncio.create_task(self._formalize(chapter)): chapter for chapter in self.chapters
        }
        had_failure = False
        try:
            while running:
                done, _ = await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    chapter = running.pop(task)
                    outcome = task.result()
                    if outcome.capacity_deferred:
                        retry = asyncio.create_task(self._formalize(chapter, rerun=True))
                        running[retry] = chapter
                    elif not outcome.succeeded:
                        had_failure = True
        except BaseException:
            for task in running:
                task.cancel()
            await asyncio.gather(*running, return_exceptions=True)
            raise
        return not had_failure

    async def _review_once(self, chapter: Chapter, *, rerun: bool = False) -> ReviewOutcome:
        if not rerun and self._already_done(chapter, Stage.REVIEW):
            return ReviewOutcome(True, False, {}, complete=True)
        attempt = await self._attempt(
            chapter,
            Stage.REVIEW,
            queue_detail="source-faithful editing review",
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
                    "review changes merged; awaiting dependency-ordered rebuild",
                    phase=TaskPhase.AWAITING_REBUILD,
                )
            return ReviewOutcome(True, attempt.agent.changed, {}, complete=True)
        detail = "review left fixup findings" if routed else "editing review failed"
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
        )

    def _review_checkpoint(self, chapter_id: str) -> str:
        clean = self.state.fixup_graph.get("clean", {})
        record = clean.get(chapter_id) if isinstance(clean, dict) else None
        certificate = record.get("certificate") if isinstance(record, dict) else None
        if not isinstance(certificate, str) or not certificate:
            raise RuntimeError(f"cannot complete review without a green certificate: {chapter_id}")
        return certificate

    async def _complete_review(self, chapter: Chapter, detail: str) -> None:
        await self.state.set_task(
            chapter.id,
            Stage.REVIEW,
            TaskStatus.SUCCEEDED,
            detail,
            checkpoint=self._review_checkpoint(chapter.id),
        )

    async def _reconcile_review_checkpoints(
        self,
        graph: ChapterImportGraph,
    ) -> set[str]:
        """Return review-complete chapters after reconciling durable certificates."""

        persisted = self.state.fixup_graph.get("clean", {})
        records = persisted if isinstance(persisted, dict) else {}
        clean = self._retain_fixup_clean(graph, records)
        reviewed: set[str] = set()
        async with self.state.batch():
            for chapter in self.chapters:
                task = self.state.task(chapter.id, Stage.REVIEW)
                record = clean.get(chapter.id)
                certificate = record.get("certificate") if isinstance(record, dict) else None
                if task.status != TaskStatus.SUCCEEDED:
                    continue
                if isinstance(certificate, str) and task.checkpoint == certificate:
                    reviewed.add(chapter.id)
                    continue
                if isinstance(certificate, str) and task.checkpoint is None:
                    # Version-7 states predate review checkpoints. A final success
                    # against a still-valid green build can be adopted once.
                    await self.state.set_task(
                        chapter.id,
                        Stage.REVIEW,
                        TaskStatus.SUCCEEDED,
                        task.detail,
                        checkpoint=certificate,
                    )
                    reviewed.add(chapter.id)
                    continue
                await self.state.set_task(
                    chapter.id,
                    Stage.REVIEW,
                    TaskStatus.PENDING,
                    "review checkpoint invalidated by source or dependency changes",
                )
        return reviewed

    async def _review_chapter_to_clean(
        self,
        chapter: Chapter,
        rounds_used: dict[str, int],
        *,
        rerun: bool = False,
    ) -> bool:
        """Run at most five edit/rebuild cycles for one dependency-ready chapter."""

        # A persisted certificate lets an unchanged chapter enter review immediately
        # after restart. If its source or any observed dependency changed, targeted
        # fixup rebuilds just the invalid dependency closure before review starts.
        review_was_done = self._already_done(chapter, Stage.REVIEW)
        async with self.fixup_lock:
            graph = self._observed_chapter_graph()
            persisted = self.state.fixup_graph.get("clean", {})
            records = persisted if isinstance(persisted, dict) else {}
            was_clean = chapter.id in self._retain_fixup_clean(graph, records)
            record = records.get(chapter.id)
            certificate = record.get("certificate") if isinstance(record, dict) else None
            review_task = self.state.task(chapter.id, Stage.REVIEW)
            if (
                review_was_done
                and isinstance(certificate, str)
                and review_task.checkpoint == certificate
                and was_clean
            ):
                return True
            if not await self._fixup_to_clean(target_ids={chapter.id}):
                return False
        rerun = rerun or (review_was_done and (not was_clean or review_task.checkpoint is not None))

        maximum = min(self.config.stages[Stage.REVIEW].max_rounds, 5)
        while rounds_used[chapter.id] < maximum:
            rounds_used[chapter.id] += 1
            outcome = await self._review_once(
                chapter,
                rerun=rerun or rounds_used[chapter.id] > 1,
            )
            if not outcome.succeeded:
                return False
            findings_by_owner: dict[str, dict[str, None]] = {}
            for owner, feedback in outcome.feedback_by_owner.items():
                findings_by_owner.setdefault(owner, {})[feedback] = None
            findings = {owner: "\n\n".join(blocks) for owner, blocks in findings_by_owner.items()}
            if outcome.changed or findings:
                targets = {chapter.id, *findings}
                async with self.fixup_lock:
                    if not await self._fixup_to_clean(findings, target_ids=targets):
                        return False
            if not outcome.complete:
                if not outcome.changed and not findings:
                    await self.state.set_task(
                        chapter.id,
                        Stage.REVIEW,
                        TaskStatus.FAILED,
                        "review was incomplete and supplied no actionable fixup",
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
            if not outcome.changed:
                detail = "editing review found no actionable issues"
                if findings:
                    detail = "no-change review findings reconciled by dependency-ordered fixup"
                await self._complete_review(chapter, detail)
                return True
        await self._complete_review(
            chapter,
            f"review/rebuild cap reached after {maximum} cycles",
        )
        return True

    async def _review_tree(self, *, rerun: bool = False, prove: bool = False) -> bool:
        """Release dependency-ready reviews and proofs without a corpus-wide review gate."""

        by_id = {chapter.id: chapter for chapter in self.chapters}
        initial_graph = self._observed_chapter_graph()
        reviewed = await self._reconcile_review_checkpoints(initial_graph)
        review_failures: set[str] = set()
        review_blocked: set[str] = set()
        attempted: set[str] = set()
        rounds_used = {chapter_id: 0 for chapter_id in by_id}
        review_tasks: dict[str, tuple[asyncio.Task[bool], frozenset[str]]] = {}
        proof_tasks: dict[str, asyncio.Task[bool]] = {}
        proof_results = {
            chapter_id: True
            for chapter_id in reviewed
            if self.state.task(chapter_id, Stage.PROVE).status == TaskStatus.SUCCEEDED
        }
        proof_fixups = {chapter_id: 0 for chapter_id in by_id}

        async def cancel_all() -> None:
            tasks = [task for task, _ in review_tasks.values()] + list(proof_tasks.values())
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        try:
            while len(reviewed) < len(by_id) or (prove and len(proof_results) < len(by_id)):
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

                for chapter_id in graph.order:
                    if (
                        chapter_id not in reviewed
                        and chapter_id not in review_failures
                        and chapter_id not in review_blocked
                        and chapter_id not in review_tasks
                        and graph.dependencies[chapter_id].issubset(reviewed)
                    ):
                        dependencies = graph.dependencies[chapter_id]
                        review_tasks[chapter_id] = (
                            asyncio.create_task(
                                self._review_chapter_to_clean(
                                    by_id[chapter_id],
                                    rounds_used,
                                    rerun=rerun or chapter_id in attempted,
                                )
                            ),
                            dependencies,
                        )
                        attempted.add(chapter_id)

                if prove:
                    for chapter_id in reviewed:
                        if chapter_id not in proof_results and chapter_id not in proof_tasks:
                            proof_tasks[chapter_id] = asyncio.create_task(
                                self._prove(by_id[chapter_id])
                            )

                live_tasks = [task for task, _ in review_tasks.values()]
                live_tasks.extend(proof_tasks.values())
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

                completed_reviews = [
                    chapter_id for chapter_id, (task, _) in review_tasks.items() if task in done
                ]
                for chapter_id in completed_reviews:
                    task, starting_dependencies = review_tasks.pop(chapter_id)
                    succeeded = task.result()
                    if not succeeded:
                        review_failures.add(chapter_id)
                        task_record = self.state.task(chapter_id, Stage.REVIEW)
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
                    if not current_dependencies.issubset(starting_dependencies):
                        continue
                    reviewed.add(chapter_id)
                    if prove:
                        proof_tasks[chapter_id] = asyncio.create_task(
                            self._prove(by_id[chapter_id])
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
                    if proof_fixups[chapter_id] >= self.config.stages[Stage.FIXUP].max_rounds:
                        proof_results[chapter_id] = False
                        continue
                    proof_fixups[chapter_id] += 1

                    chapter = by_id[chapter_id]
                    findings = self._route_review_findings(chapter, report)
                    targets = {chapter_id, *findings}

                    current_graph = self._observed_chapter_graph()
                    invalidated = set(targets)
                    pending_invalidated = list(targets)
                    while pending_invalidated:
                        owner = pending_invalidated.pop()
                        for successor in current_graph.successors.get(owner, frozenset()):
                            if successor not in invalidated:
                                invalidated.add(successor)
                                pending_invalidated.append(successor)

                    cancelled: list[asyncio.Task[bool]] = []
                    for invalidated_id in invalidated:
                        if handle := review_tasks.pop(invalidated_id, None):
                            handle[0].cancel()
                            cancelled.append(handle[0])
                        if task := proof_tasks.pop(invalidated_id, None):
                            task.cancel()
                            cancelled.append(task)
                        reviewed.discard(invalidated_id)
                        proof_results.pop(invalidated_id, None)
                        rounds_used[invalidated_id] = 0
                    async with self.state.batch():
                        await self.state.set_tasks(
                            invalidated,
                            Stage.REVIEW,
                            TaskStatus.PENDING,
                            "review invalidated by proof-requested statement fixup",
                        )
                        await self.state.set_tasks(
                            invalidated,
                            Stage.PROVE,
                            TaskStatus.PENDING,
                            "waiting for invalidated statement review",
                        )
                    await asyncio.gather(*cancelled, return_exceptions=True)

                    async with self.fixup_lock:
                        if not await self._fixup_to_clean(findings, target_ids=targets):
                            return False
        finally:
            await cancel_all()

        return (
            not review_failures
            and not review_blocked
            and (not prove or all(proof_results.values()))
        )

    async def _review_until_clean(self, *, rerun: bool = False) -> bool:
        return await self._review_tree(rerun=rerun)

    async def _prove(self, chapter: Chapter) -> bool:
        if self._already_done(chapter, Stage.PROVE):
            return True
        proof_maximum = self.config.stages[Stage.PROVE].max_rounds
        feedback = ""
        for proof_round in range(1, proof_maximum + 1):
            attempt = await self._attempt(
                chapter,
                Stage.PROVE,
                feedback=feedback,
                queue_detail=f"proof round {proof_round}/{proof_maximum}",
            )
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
                )
                return True

            feedback = attempt.feedback()
            if bool(attempt.agent.report.get("fixup_findings")):
                await self.state.set_task(
                    chapter.id,
                    Stage.PROVE,
                    TaskStatus.FAILED,
                    "proof agent requested statement fixup",
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
        if not await self._formalize_all():
            return False
        return await self._review_tree(prove=True)
