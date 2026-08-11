from __future__ import annotations

import asyncio
import heapq
import re
from collections.abc import AsyncIterator, Callable, Coroutine, Iterable
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from typing import Any

from lastlib_swarm.codex import AgentResult, CodexExecutor, ValidationResult, validate
from lastlib_swarm.corpus import build_corpus_schedule, scheduling_snapshot
from lastlib_swarm.diagnostics import unexpected_lean_warnings
from lastlib_swarm.isolation import IsolationResult, create_isolation
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
    needs_fixup: bool
    feedback: str


@dataclass(frozen=True)
class BuildDiagnostics:
    actionable: dict[str, str]
    deferred_owner_ids: tuple[str, ...]


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

    def scheduling_snapshot(self) -> dict[str, object]:
        return scheduling_snapshot(self.statement_schedule, self.proof_schedule)

    async def prepare(self) -> None:
        await self.state.load_or_create()
        await self.executor.prepare()
        self.scaffold()
        await self.isolation.prepare()

    async def shutdown(self) -> None:
        await self.isolation.close()

    def _already_done(self, chapter: Chapter, stage: Stage) -> bool:
        return not self.force and self.state.task(chapter.id, stage).status == TaskStatus.SUCCEEDED

    def scaffold(self) -> None:
        """Create configured chapter directories without creating Lean files."""

        scaffold_directories(self.config, self.chapters)

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
                if stage is Stage.REVIEW and agent.changed:
                    isolated = IsolationResult(
                        accepted=False,
                        generation=getattr(workspace, "generation", 0),
                        cache_generation=getattr(workspace, "cache_generation", 0),
                        error="read-only review attempted to change files",
                    )
                else:
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
                results[chapter.id] = await validate(
                    self.config,
                    chapter,
                    workspace_root=self.config.settings.repo,
                    on_output=self.state.append_coordinator_build_output,
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

    async def _fixup_agent(self, chapter: Chapter, feedback: str) -> bool:
        maximum = self.config.stages[Stage.FIXUP].max_rounds
        task = self.state.task(chapter.id, Stage.FIXUP)
        attempt = await self._attempt(
            chapter,
            Stage.FIXUP,
            feedback=feedback,
            queue_detail=f"fixup attempt {task.rounds + 1} (global cap {maximum})",
        )
        if attempt.agent.succeeded and attempt.validation.succeeded:
            await self.state.set_task(
                chapter.id,
                Stage.FIXUP,
                TaskStatus.RUNNING,
                "fixup complete; awaiting coordinator rebuild",
                phase=TaskPhase.AWAITING_REBUILD,
            )
            return True
        await self.state.set_task(
            chapter.id,
            Stage.FIXUP,
            TaskStatus.FAILED,
            "fixup agent failed",
        )
        return False

    async def _fixup_to_clean(self, feedback: dict[str, str] | None = None) -> bool:
        pending = dict(feedback or {})
        by_id = {chapter.id: chapter for chapter in self.chapters}
        maximum = self.config.stages[Stage.FIXUP].max_rounds
        for iteration in range(1, maximum + 1):
            if pending:
                outcomes = await _gather_cancel_on_error(
                    self._fixup_agent(by_id[chapter_id], diagnostic)
                    for chapter_id, diagnostic in pending.items()
                    if chapter_id in by_id
                )
                if not all(outcomes):
                    return False
            results = await self._build_all(
                iteration=iteration,
                maximum_iterations=maximum,
            )
            if all(result.succeeded for result in results.values()):
                for chapter in self.chapters:
                    await self.state.set_task(
                        chapter.id,
                        Stage.FIXUP,
                        TaskStatus.SUCCEEDED,
                        f"clean coordinator build after {iteration} iteration(s)",
                    )
                return True
            pending = self._build_feedback(results).actionable
            if not pending:
                break
        for chapter in self.chapters:
            await self.state.set_task(
                chapter.id,
                Stage.FIXUP,
                TaskStatus.FAILED,
                f"global build did not become clean in {maximum} iterations",
            )
        return False

    async def _opportunistic_fixup(
        self,
        completed_ids: set[str],
        active_formalizer_ids: set[str],
        *,
        newer_drafts_ready: Callable[[], bool],
    ) -> bool:
        """Build and fix completed drafts while unrelated formalizers keep running."""

        completed = tuple(chapter for chapter in self.chapters if chapter.id in completed_ids)
        if not completed:
            return True
        maximum = self.config.stages[Stage.FIXUP].max_rounds
        deferred = False
        for iteration in range(1, maximum + 1):
            results = await self._build_chapters(
                completed,
                publish_if_clean=False,
                mode="streaming",
                iteration=iteration,
                maximum_iterations=maximum,
            )
            if all(result.succeeded for result in results.values()):
                return True

            diagnostics = self._build_feedback(
                results,
                blocked_owner_ids=active_formalizer_ids,
            )
            deferred = bool(diagnostics.deferred_owner_ids)
            if newer_drafts_ready():
                return True
            if not diagnostics.actionable:
                return deferred
            by_id = {chapter.id: chapter for chapter in completed}
            outcomes = await _gather_cancel_on_error(
                self._fixup_agent(by_id[chapter_id], feedback)
                for chapter_id, feedback in diagnostics.actionable.items()
                if chapter_id in by_id
            )
            if not all(outcomes):
                return False
            if newer_drafts_ready():
                return True
        return deferred

    async def _formalize_all(self, *, streaming_fixup: bool) -> bool:
        """Run every formalizer, requeueing capacity-deferred chapters at the back."""

        running = {
            asyncio.create_task(self._formalize(chapter)): chapter for chapter in self.chapters
        }
        completed_ids: set[str] = set()
        had_failure = False
        try:
            while running:
                done, _ = await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    chapter = running.pop(task)
                    outcome = task.result()
                    if outcome.succeeded:
                        completed_ids.add(chapter.id)
                    elif outcome.capacity_deferred:
                        retry = asyncio.create_task(self._formalize(chapter, rerun=True))
                        running[retry] = chapter
                    else:
                        had_failure = True
                if not streaming_fixup:
                    continue
                active_ids = {chapter.id for chapter in running.values()}
                if not await self._opportunistic_fixup(
                    completed_ids,
                    active_ids,
                    newer_drafts_ready=lambda: any(task.done() for task in running),
                ):
                    had_failure = True
        except BaseException:
            for task in running:
                task.cancel()
            await asyncio.gather(*running, return_exceptions=True)
            raise
        return not had_failure

    async def _formalize_with_streaming_fixup(self) -> bool:
        """Stream completed formalizations through targeted build/fixup batches."""

        return await self._formalize_all(streaming_fixup=True)

    async def _review_once(self, chapter: Chapter, *, rerun: bool = False) -> ReviewOutcome:
        if not rerun and self._already_done(chapter, Stage.REVIEW):
            return ReviewOutcome(True, False, "")
        attempt = await self._attempt(
            chapter,
            Stage.REVIEW,
            queue_detail="read-only review",
        )
        succeeded = attempt.agent.succeeded and attempt.validation.succeeded
        needs_fixup = bool(attempt.agent.report.get("needs_fixup"))
        complete = bool(attempt.agent.report.get("complete"))
        feedback = attempt.feedback()
        if succeeded and complete and not needs_fixup:
            await self.state.set_task(
                chapter.id,
                Stage.REVIEW,
                TaskStatus.SUCCEEDED,
                "read-only review found no actionable issues",
            )
            return ReviewOutcome(True, False, feedback)
        detail = "review reported fixup findings" if needs_fixup else "read-only review failed"
        await self.state.set_task(
            chapter.id,
            Stage.REVIEW,
            TaskStatus.FAILED,
            detail,
        )
        return ReviewOutcome(succeeded and complete, needs_fixup, feedback)

    async def _review_until_clean(self, *, rerun: bool = False) -> bool:
        maximum = self.config.stages[Stage.REVIEW].max_rounds
        for cycle in range(1, maximum + 1):
            outcomes = await asyncio.gather(
                *(self._review_once(chapter, rerun=rerun or cycle > 1) for chapter in self.chapters)
            )
            if not all(outcome.succeeded for outcome in outcomes):
                return False
            findings = {
                chapter.id: outcome.feedback
                for chapter, outcome in zip(self.chapters, outcomes, strict=True)
                if outcome.needs_fixup
            }
            if not findings:
                return True
            if not await self._fixup_to_clean(findings):
                return False
        return False

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
            if bool(attempt.agent.report.get("needs_fixup")):
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
            outcomes = await asyncio.gather(
                *(self._review_once(chapter) for chapter in self.chapters)
            )
            return all(outcome.succeeded and not outcome.needs_fixup for outcome in outcomes)
        if stage is Stage.FORMALIZE:
            return await self._formalize_all(streaming_fixup=False)
        return all(await _gather_cancel_on_error(self._prove(chapter) for chapter in self.chapters))

    async def run_pipeline(self) -> bool:
        if not await self._formalize_with_streaming_fixup():
            return False
        if not await self._fixup_to_clean():
            return False
        if not await self._review_until_clean():
            return False

        maximum = self.config.stages[Stage.FIXUP].max_rounds
        for _ in range(maximum):
            proof_results = await _gather_cancel_on_error(
                self._prove(chapter) for chapter in self.chapters
            )
            if all(proof_results):
                return True
            findings: dict[str, str] = {}
            for chapter in self.chapters:
                task = self.state.task(chapter.id, Stage.PROVE)
                if not task.runs:
                    continue
                report = task.runs[-1].report or {}
                if report.get("needs_fixup"):
                    issues = report.get("issues") or []
                    findings[chapter.id] = "Proof-stage fixup request:\n" + "\n".join(
                        f"- {issue}" for issue in issues
                    )
            if not findings:
                return False
            if not await self._fixup_to_clean(findings):
                return False
            if not await self._review_until_clean(rerun=True):
                return False
        return False
