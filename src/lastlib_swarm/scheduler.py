from __future__ import annotations

import asyncio
import heapq
from collections.abc import AsyncIterator, Callable, Coroutine, Iterable
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from typing import Any

from lastlib_swarm.codex import AgentResult, CodexExecutor, ValidationResult, validate
from lastlib_swarm.corpus import CorpusSchedule, build_corpus_schedule, scheduling_snapshot
from lastlib_swarm.isolation import create_isolation
from lastlib_swarm.models import Chapter, PipelineConfig, Stage
from lastlib_swarm.state import RunRecord, StateStore, TaskStatus


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

    def scheduling_snapshot(self) -> dict[str, object]:
        return scheduling_snapshot(self.statement_schedule, self.proof_schedule)

    async def prepare(self) -> None:
        await self.state.load_or_create()
        await self.executor.prepare()
        await self.isolation.prepare()

    async def shutdown(self) -> None:
        await self.isolation.close()

    def _already_done(self, chapter: Chapter, stage: Stage) -> bool:
        return not self.force and self.state.task(chapter.id, stage).status == TaskStatus.SUCCEEDED

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
            workspace = await self.isolation.acquire(run.id)
            try:
                agent = await self.executor.run(
                    chapter,
                    stage,
                    run,
                    feedback=feedback,
                    workspace_root=workspace.root,
                )
                validation = await validate(
                    self.config,
                    chapter,
                    workspace_root=workspace.root,
                )
                isolated = await workspace.collect(chapter)
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
            finally:
                await workspace.close()
            return Attempt(agent=agent, validation=validation, run=run)

    async def _formalize(self, chapter: Chapter) -> bool:
        if self._already_done(chapter, Stage.FORMALIZE):
            return True
        feedback = ""
        maximum = self.config.stages[Stage.FORMALIZE].max_rounds
        for round_number in range(1, maximum + 1):
            await self.state.set_task(
                chapter.id,
                Stage.FORMALIZE,
                TaskStatus.RUNNING,
                f"agent round {round_number}/{maximum}",
            )
            attempt = await self._attempt(chapter, Stage.FORMALIZE, feedback=feedback)
            if attempt.agent.succeeded and attempt.validation.succeeded:
                await self.state.set_task(
                    chapter.id,
                    Stage.FORMALIZE,
                    TaskStatus.SUCCEEDED,
                    "chapter statements elaborate",
                )
                return True
            feedback = attempt.feedback()
        await self.state.set_task(
            chapter.id,
            Stage.FORMALIZE,
            TaskStatus.FAILED,
            f"did not elaborate after {maximum} rounds",
        )
        return False

    async def _review(self, chapter: Chapter) -> bool:
        if self._already_done(chapter, Stage.REVIEW):
            return True
        feedback = ""
        maximum = self.config.stages[Stage.REVIEW].max_rounds
        for round_number in range(1, maximum + 1):
            await self.state.set_task(
                chapter.id,
                Stage.REVIEW,
                TaskStatus.RUNNING,
                f"fixed-point round {round_number}/{maximum}",
            )
            attempt = await self._attempt(chapter, Stage.REVIEW, feedback=feedback)
            converged = (
                attempt.agent.succeeded
                and attempt.validation.succeeded
                and not attempt.agent.changed
            )
            if converged:
                await self.state.set_task(
                    chapter.id,
                    Stage.REVIEW,
                    TaskStatus.SUCCEEDED,
                    "independent review made no changes",
                )
                return True
            feedback = attempt.feedback()
        await self.state.set_task(
            chapter.id,
            Stage.REVIEW,
            TaskStatus.FAILED,
            f"review did not reach a no-change fixed point in {maximum} rounds",
        )
        return False

    async def _repair_once(self, chapter: Chapter, feedback: str) -> Attempt:
        maximum = self.config.stages[Stage.REPAIR].max_rounds
        task = self.state.task(chapter.id, Stage.REPAIR)
        next_round = task.rounds + 1
        await self.state.set_task(
            chapter.id,
            Stage.REPAIR,
            TaskStatus.RUNNING,
            f"statement repair attempt {next_round} (run cap {maximum})",
        )
        attempt = await self._attempt(chapter, Stage.REPAIR, feedback=feedback)
        status = TaskStatus.SUCCEEDED if attempt.agent.succeeded else TaskStatus.FAILED
        detail = "repair pass completed" if attempt.agent.succeeded else "repair agent failed"
        await self.state.set_task(chapter.id, Stage.REPAIR, status, detail)
        return attempt

    async def _prove(self, chapter: Chapter, *, allow_repair: bool) -> bool:
        if self._already_done(chapter, Stage.PROVE):
            return True
        proof_maximum = self.config.stages[Stage.PROVE].max_rounds
        repair_maximum = self.config.stages[Stage.REPAIR].max_rounds
        feedback = ""
        repair_rounds = 0
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
            needs_repair = bool(attempt.agent.report.get("needs_repair"))
            stalled = attempt.agent.succeeded and not attempt.agent.changed
            if allow_repair and (needs_repair or stalled):
                if repair_rounds >= repair_maximum:
                    break
                repair_rounds += 1
                repair = await self._repair_once(chapter, feedback)
                feedback = repair.feedback()
                if not repair.agent.succeeded:
                    continue
            elif needs_repair and not allow_repair:
                await self.state.set_task(
                    chapter.id,
                    Stage.PROVE,
                    TaskStatus.FAILED,
                    "proof agent requested statement repair",
                )
                return False

        await self.state.set_task(
            chapter.id,
            Stage.PROVE,
            TaskStatus.FAILED,
            f"proof/repair did not converge in {proof_maximum} proof rounds",
        )
        return False

    async def _repair_to_fixed_point(self, chapter: Chapter) -> bool:
        if self._already_done(chapter, Stage.REPAIR):
            return True
        feedback = ""
        maximum = self.config.stages[Stage.REPAIR].max_rounds
        for _ in range(maximum):
            attempt = await self._repair_once(chapter, feedback)
            if (
                attempt.agent.succeeded
                and attempt.validation.succeeded
                and not attempt.agent.changed
            ):
                await self.state.set_task(
                    chapter.id,
                    Stage.REPAIR,
                    TaskStatus.SUCCEEDED,
                    "repair review made no changes",
                )
                return True
            feedback = attempt.feedback()
        await self.state.set_task(
            chapter.id,
            Stage.REPAIR,
            TaskStatus.FAILED,
            f"repair did not reach a no-change fixed point in {maximum} rounds",
        )
        return False

    async def run_stage(self, stage: Stage) -> bool:
        operations = {
            Stage.FORMALIZE: self._formalize,
            Stage.REVIEW: self._review,
            Stage.PROVE: lambda chapter: self._prove(chapter, allow_repair=False),
            Stage.REPAIR: self._repair_to_fixed_point,
        }

        async def run_book(book_id: str) -> bool:
            chapters = [chapter for chapter in self.chapters if chapter.book_id == book_id]
            results = await asyncio.gather(*(operations[stage](chapter) for chapter in chapters))
            return all(results)

        schedule = (
            self.statement_schedule
            if stage in (Stage.FORMALIZE, Stage.REVIEW)
            else self.proof_schedule
        )
        succeeded = await self._run_book_graph(schedule, run_book, stages=(stage,))
        return succeeded == set(schedule.order)

    async def _statement_chapter(self, chapter: Chapter) -> bool:
        if not await self._formalize(chapter):
            await self.state.set_task(
                chapter.id,
                Stage.REVIEW,
                TaskStatus.BLOCKED,
                "formalization failed",
            )
            return False
        return await self._review(chapter)

    async def _statement_book(self, book_id: str) -> bool:
        chapters = [chapter for chapter in self.chapters if chapter.book_id == book_id]
        results = await asyncio.gather(*(self._statement_chapter(chapter) for chapter in chapters))
        return all(results)

    async def _proof_book(self, book_id: str) -> bool:
        chapters = [chapter for chapter in self.chapters if chapter.book_id == book_id]
        results = await asyncio.gather(
            *(self._prove(chapter, allow_repair=True) for chapter in chapters)
        )
        return all(results)

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
        return succeeded

    async def _run_statements(self) -> set[str]:
        return await self._run_book_graph(
            self.statement_schedule,
            self._statement_book,
            stages=(Stage.FORMALIZE, Stage.REVIEW),
        )

    async def run_pipeline(self) -> bool:
        statement_books = await self._run_statements()
        selected_books = {chapter.book_id for chapter in self.chapters}
        for chapter in self.chapters:
            if chapter.book_id not in statement_books:
                for stage in (Stage.PROVE, Stage.REPAIR):
                    await self.state.set_task(
                        chapter.id,
                        stage,
                        TaskStatus.BLOCKED,
                        "book did not complete statement review",
                    )
        proof_schedule = build_corpus_schedule(
            self.config.books,
            self.chapters,
            phase="proofs",
            selected_books=statement_books,
        )
        proof_books = await self._run_book_graph(
            proof_schedule,
            self._proof_book,
            stages=(Stage.PROVE, Stage.REPAIR),
        )
        return statement_books == selected_books and proof_books == selected_books
