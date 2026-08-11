from __future__ import annotations

import asyncio
import heapq
from collections.abc import AsyncIterator, Callable, Coroutine, Iterable
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from typing import Any

from lastlib_swarm.codex import AgentResult, CodexExecutor, ValidationResult, validate
from lastlib_swarm.corpus import CorpusSchedule, build_corpus_schedule, scheduling_snapshot
from lastlib_swarm.isolation import IsolationResult, create_isolation
from lastlib_swarm.models import Chapter, PipelineConfig, Stage
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
class ReviewOutcome:
    succeeded: bool
    needs_fixup: bool
    feedback: str


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
    ) -> Attempt:
        await self.control.checkpoint()
        schedule = (
            self.statement_schedule
            if stage in (Stage.FORMALIZE, Stage.REVIEW)
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

    async def _formalize(self, chapter: Chapter) -> bool:
        if self._already_done(chapter, Stage.FORMALIZE):
            return True
        if not self.force and self._scope_exists(chapter):
            await self.state.set_task(
                chapter.id,
                Stage.FORMALIZE,
                TaskStatus.SUCCEEDED,
                "existing chapter files skipped",
            )
            return True
        await self.state.set_task(
            chapter.id,
            Stage.FORMALIZE,
            TaskStatus.RUNNING,
            "single optimistic drafting pass",
        )
        attempt = await self._attempt(chapter, Stage.FORMALIZE)
        if attempt.agent.succeeded and attempt.validation.succeeded:
            await self.state.set_task(
                chapter.id,
                Stage.FORMALIZE,
                TaskStatus.SUCCEEDED,
                "optimistic chapter draft completed",
            )
            return True
        await self.state.set_task(
            chapter.id,
            Stage.FORMALIZE,
            TaskStatus.FAILED,
            "single drafting attempt failed",
        )
        return False

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

    async def _build_all(self) -> dict[str, ValidationResult]:
        """Run every selected chapter build against the coordinator-owned cache."""

        results: dict[str, ValidationResult] = {}
        await self.build_queue.acquire()
        try:
            for chapter in self.chapters:
                await self.control.checkpoint()
                results[chapter.id] = await validate(
                    self.config,
                    chapter,
                    workspace_root=self.config.settings.repo,
                )
            if all(result.succeeded for result in results.values()):
                await self.isolation.refresh_cache()
        finally:
            self.build_queue.release()
        return results

    def _build_feedback(self, results: dict[str, ValidationResult]) -> dict[str, str]:
        feedback: dict[str, list[str]] = {}
        by_id = {chapter.id: chapter for chapter in self.chapters}
        for target_id, result in results.items():
            if result.succeeded:
                continue
            owners = [
                chapter.id
                for chapter in self.chapters
                if any(
                    identifier in result.output for identifier in self._chapter_identifiers(chapter)
                )
            ]
            if not owners:
                owners = [target_id]
            block = (
                f"Coordinator build of {by_id[target_id].chapter_module} failed:\n{result.output}"
            )
            for owner in owners:
                feedback.setdefault(owner, []).append(block)
        return {chapter_id: "\n\n".join(blocks) for chapter_id, blocks in feedback.items()}

    async def _fixup_agent(self, chapter: Chapter, feedback: str) -> bool:
        maximum = self.config.stages[Stage.FIXUP].max_rounds
        task = self.state.task(chapter.id, Stage.FIXUP)
        await self.state.set_task(
            chapter.id,
            Stage.FIXUP,
            TaskStatus.RUNNING,
            f"fixup attempt {task.rounds + 1} (global cap {maximum})",
        )
        attempt = await self._attempt(chapter, Stage.FIXUP, feedback=feedback)
        if attempt.agent.succeeded and attempt.validation.succeeded:
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
            for chapter in self.chapters:
                await self.state.set_task(
                    chapter.id,
                    Stage.FIXUP,
                    TaskStatus.RUNNING,
                    f"coordinator build iteration {iteration}/{maximum}",
                )
            results = await self._build_all()
            if all(result.succeeded for result in results.values()):
                for chapter in self.chapters:
                    await self.state.set_task(
                        chapter.id,
                        Stage.FIXUP,
                        TaskStatus.SUCCEEDED,
                        f"clean coordinator build after {iteration} iteration(s)",
                    )
                return True
            pending = self._build_feedback(results)
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

    async def _review_once(self, chapter: Chapter, *, rerun: bool = False) -> ReviewOutcome:
        if not rerun and self._already_done(chapter, Stage.REVIEW):
            return ReviewOutcome(True, False, "")
        await self.state.set_task(
            chapter.id,
            Stage.REVIEW,
            TaskStatus.RUNNING,
            "read-only review",
        )
        attempt = await self._attempt(chapter, Stage.REVIEW)
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
            await self.state.set_task(
                chapter.id,
                Stage.PROVE,
                TaskStatus.RUNNING,
                f"proof round {proof_round}/{proof_maximum}",
            )
            attempt = await self._attempt(chapter, Stage.PROVE, feedback=feedback)
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
        operation = self._formalize if stage is Stage.FORMALIZE else self._prove
        return all(await _gather_cancel_on_error(operation(chapter) for chapter in self.chapters))

    async def _run_book_graph(
        self,
        schedule: CorpusSchedule,
        operation: Callable[[str], Coroutine[Any, Any, bool]],
        *,
        stages: tuple[Stage, ...],
    ) -> set[str]:
        pending = set(schedule.order)
        succeeded: set[str] = set()
        running: dict[asyncio.Task[bool], str] = {}
        try:
            while pending or running:
                for book_id in schedule.order:
                    if book_id in pending and schedule.dependencies[book_id] <= succeeded:
                        running[asyncio.create_task(operation(book_id))] = book_id
                        pending.remove(book_id)
                if not running:
                    if pending:
                        for chapter in self.chapters:
                            if chapter.book_id in pending:
                                for stage in stages:
                                    await self.state.set_task(
                                        chapter.id,
                                        stage,
                                        TaskStatus.BLOCKED,
                                        "upstream book did not complete successfully",
                                    )
                        pending.clear()
                    break
                done, _ = await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    book_id = running.pop(task)
                    if task.result():
                        succeeded.add(book_id)
        except BaseException:
            for task in running:
                task.cancel()
            await asyncio.gather(*running, return_exceptions=True)
            raise
        return succeeded

    async def run_pipeline(self) -> bool:
        if not all(
            await _gather_cancel_on_error(self._formalize(chapter) for chapter in self.chapters)
        ):
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
