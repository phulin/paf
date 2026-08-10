from __future__ import annotations

import asyncio
from collections.abc import Iterable
from dataclasses import dataclass

from lastlib_swarm.codex import AgentResult, CodexExecutor, ValidationResult, validate
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


class Orchestrator:
    def __init__(
        self,
        config: PipelineConfig,
        state: StateStore,
        *,
        chapters: Iterable[Chapter] | None = None,
        force: bool = False,
    ) -> None:
        self.config = config
        self.state = state
        self.chapters = tuple(chapters if chapters is not None else config.chapters)
        self.force = force
        self.executor = CodexExecutor(config, state)
        self.agent_slots = asyncio.Semaphore(config.settings.max_agents)

    async def prepare(self) -> None:
        await self.state.load_or_create()
        await self.executor.prepare()

    def _already_done(self, chapter: Chapter, stage: Stage) -> bool:
        return not self.force and self.state.task(chapter.id, stage).status == TaskStatus.SUCCEEDED

    async def _attempt(
        self,
        chapter: Chapter,
        stage: Stage,
        *,
        feedback: str = "",
    ) -> Attempt:
        async with self.agent_slots:
            run = await self.state.start_run(chapter.id, stage)
            agent = await self.executor.run(chapter, stage, run, feedback=feedback)
            validation = await validate(self.config, chapter)
            await self.state.update_run(run, validation=validation.as_dict())
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
        results = await asyncio.gather(*(operations[stage](chapter) for chapter in self.chapters))
        return all(results)

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

    async def _run_statements(self) -> set[str]:
        selected_books = {chapter.book_id for chapter in self.chapters}
        dependencies = {
            book.id: set(book.depends_on) & selected_books
            for book in self.config.books
            if book.id in selected_books
        }
        pending = set(selected_books)
        succeeded: set[str] = set()
        failed: set[str] = set()
        running: dict[asyncio.Task[bool], str] = {}
        while pending or running:
            launched = False
            for book_id in sorted(pending):
                if dependencies[book_id] <= succeeded:
                    running[asyncio.create_task(self._statement_book(book_id))] = book_id
                    pending.remove(book_id)
                    launched = True
            if not running:
                if pending:
                    for chapter in self.chapters:
                        if chapter.book_id in pending:
                            for stage in (Stage.FORMALIZE, Stage.REVIEW):
                                await self.state.set_task(
                                    chapter.id,
                                    stage,
                                    TaskStatus.BLOCKED,
                                    "book dependency failed or is cyclic",
                                )
                    failed.update(pending)
                    pending.clear()
                break
            if launched and len(running) < len(selected_books):
                await asyncio.sleep(0)
            done, _ = await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                book_id = running.pop(task)
                if task.result():
                    succeeded.add(book_id)
                else:
                    failed.add(book_id)
        return succeeded

    async def run_pipeline(self) -> bool:
        statement_books = await self._run_statements()
        proof_chapters = [
            chapter for chapter in self.chapters if chapter.book_id in statement_books
        ]
        proof_results = await asyncio.gather(
            *(self._prove(chapter, allow_repair=True) for chapter in proof_chapters)
        )
        return len(statement_books) == len({chapter.book_id for chapter in self.chapters}) and all(
            proof_results
        )
