from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from pathlib import Path

from paf.models import Stage, WorkUnitLike
from paf.scope import ScopeMatcher


class GitCommitError(RuntimeError):
    """A coordinator-owned Git operation could not be completed safely."""


@dataclass(frozen=True)
class GitResult:
    exit_code: int
    output: str


def _book_label(book_id: str) -> str:
    match = re.search(r"\d+", book_id)
    return str(int(match.group())) if match else book_id


def agent_commit_subject(chapter: WorkUnitLike, stage: Stage) -> str:
    """Build the stable Conventional Commit subject for one agent patch."""

    book = _book_label(chapter.book_id)
    return (
        f"chore({chapter.book_id}): changes from {stage.value} agent "
        f"on book {book} chapter {chapter.number}"
    )


def deterministic_warning_commit_subject(chapter: WorkUnitLike) -> str:
    """Build the stable subject for a coordinator-owned warning cleanup."""

    book = _book_label(chapter.book_id)
    return (
        f"chore({chapter.book_id}): resolve deterministic warnings "
        f"on book {book} chapter {chapter.number}"
    )


def deterministic_warning_revert_subject(chapter: WorkUnitLike) -> str:
    """Build the stable subject for rolling back a failed deterministic cleanup."""

    book = _book_label(chapter.book_id)
    return f"revert({chapter.book_id}): restore warnings on book {book} chapter {chapter.number}"


class GitCommitter:
    """Create exact, coordinator-owned commits without absorbing unrelated work."""

    def __init__(self, repo: Path) -> None:
        self.repo = repo
        self.enabled = False

    async def _run(self, *arguments: str) -> GitResult:
        process = await asyncio.create_subprocess_exec(
            "git",
            "--literal-pathspecs",
            *arguments,
            cwd=self.repo,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        output, _ = await process.communicate()
        return GitResult(process.returncode or 0, output.decode(errors="replace"))

    async def _checked(self, *arguments: str) -> str:
        result = await self._run(*arguments)
        if result.exit_code:
            command = "git " + " ".join(arguments[:2])
            raise GitCommitError(
                f"{command} failed ({result.exit_code}): {result.output[-4000:].strip()}"
            )
        return result.output

    async def prepare(self) -> None:
        result = await self._run("rev-parse", "--show-toplevel")
        if result.exit_code:
            # Explicit test fixtures and exported source archives are allowed
            # to run without Git. A real worktree must commit every patch.
            self.enabled = False
            return
        top_level = Path(result.output.strip()).resolve()
        if top_level != self.repo.resolve():
            raise GitCommitError(
                f"configured repository {self.repo} is inside Git worktree {top_level}; "
                "agent commits require the configured repository to be the worktree root"
            )
        await self._checked("rev-parse", "--verify", "HEAD")
        self.enabled = True

    async def dirty_paths(self, chapter: WorkUnitLike) -> tuple[str, ...]:
        if not self.enabled:
            return ()
        paths = await self.working_tree_paths()
        matcher = ScopeMatcher(chapter.scope)
        return tuple(path for path in paths if matcher.matches(path))

    async def working_tree_paths(self) -> tuple[str, ...]:
        """Return all uncommitted paths with two repository-wide Git queries."""

        if not self.enabled:
            return ()
        tracked, untracked = await asyncio.gather(
            self._checked("diff", "--name-only", "--no-renames", "-z", "HEAD", "--"),
            self._checked("ls-files", "--others", "--exclude-standard", "-z", "--"),
        )
        paths = {path for output in (tracked, untracked) for path in output.split("\0") if path}
        return tuple(sorted(paths))

    async def ensure_clean(self, chapter: WorkUnitLike) -> None:
        dirty = await self.dirty_paths(chapter)
        if dirty:
            raise GitCommitError(
                f"cannot start {chapter.id} with uncommitted files in its exclusive scope: "
                + ", ".join(dirty)
            )

    async def head(self) -> str:
        """Return the canonical repository revision."""

        return (await self._checked("rev-parse", "HEAD")).strip() if self.enabled else ""

    async def commit(
        self,
        chapter: WorkUnitLike,
        stage: Stage,
        *,
        summary: str,
        changed_paths: tuple[str, ...],
        subject: str | None = None,
    ) -> str:
        if not self.enabled or not changed_paths:
            return ""
        body = summary.strip()
        if not body:
            raise GitCommitError("agent changed source but returned an empty summary")
        matcher = ScopeMatcher(chapter.scope)
        paths = tuple(dict.fromkeys(changed_paths))
        outside = tuple(path for path in paths if not matcher.matches(path))
        if outside:
            raise GitCommitError(
                "refusing to commit paths outside the agent's exclusive scope: "
                + ", ".join(outside)
            )

        await self._checked("add", "-A", "--", *paths)
        staged = await self._run("diff", "--cached", "--quiet", "HEAD", "--", *paths)
        if staged.exit_code == 0:
            return ""
        if staged.exit_code != 1:
            raise GitCommitError(
                f"git diff --cached failed ({staged.exit_code}): {staged.output[-4000:].strip()}"
            )
        await self._checked(
            "commit",
            "--only",
            "--no-verify",
            "--no-gpg-sign",
            "-m",
            subject or agent_commit_subject(chapter, stage),
            "-m",
            body,
            "--",
            *paths,
        )
        return (await self._checked("rev-parse", "HEAD")).strip()
