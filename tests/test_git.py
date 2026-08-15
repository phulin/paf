from __future__ import annotations

import subprocess
from dataclasses import replace
from pathlib import Path

import pytest

from paf.codex import AgentResult, CodexExecutor
from paf.config import load_config
from paf.git import GitCommitError, GitCommitter, agent_commit_subject
from paf.models import Chapter, Stage
from paf.scheduler import Orchestrator
from paf.state import RunRecord, StateStore, TaskStatus, TokenUsage
from tests.support import write_project


def git(repo: Path, *arguments: str) -> str:
    return subprocess.run(
        ("git", *arguments),
        cwd=repo,
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    ).stdout


def initialize(repo: Path) -> None:
    git(repo, "init", "-q")
    git(repo, "config", "user.name", "PAF Test")
    git(repo, "config", "user.email", "paf@example.com")
    git(repo, "add", "-A")
    git(repo, "commit", "-qm", "chore: initialize fixture")


@pytest.mark.asyncio
async def test_commits_only_agent_paths_with_summary_body(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = replace(config.chapters[0], book_id="book07")
    chapter_root = tmp_path / "lean" / "Book" / "Chapter01"
    chapter_root.mkdir(parents=True)
    aggregator = tmp_path / "lean" / "Book" / "Chapter01.lean"
    modified = chapter_root / "Modified.lean"
    deleted = chapter_root / "Deleted.lean"
    unrelated_staged = tmp_path / "staged.txt"
    unrelated_unstaged = tmp_path / "unstaged.txt"
    aggregator.write_text("import Book.Chapter01.Modified\n", encoding="utf-8")
    modified.write_text("def value := 1\n", encoding="utf-8")
    deleted.write_text("def obsolete := 1\n", encoding="utf-8")
    unrelated_staged.write_text("before\n", encoding="utf-8")
    unrelated_unstaged.write_text("before\n", encoding="utf-8")
    initialize(tmp_path)

    committer = GitCommitter(tmp_path)
    await committer.prepare()
    await committer.ensure_clean(chapter)

    modified.write_text("def value := 2\n", encoding="utf-8")
    deleted.unlink()
    created = chapter_root / "New theorem - exact.lean"
    created.write_text("theorem added : True := by trivial\n", encoding="utf-8")
    unrelated_staged.write_text("staged\n", encoding="utf-8")
    git(tmp_path, "add", "staged.txt")
    unrelated_unstaged.write_text("unstaged\n", encoding="utf-8")

    summary = "Updated `value` and added the chapter theorem.\n\nRemoved the obsolete declaration."
    changed = tuple(path.relative_to(tmp_path).as_posix() for path in (modified, deleted, created))
    commit = await committer.commit(
        chapter,
        Stage.REVIEW,
        summary=summary,
        changed_paths=changed,
    )

    assert commit == git(tmp_path, "rev-parse", "HEAD").strip()
    assert git(tmp_path, "show", "-s", "--format=%s", "HEAD").strip() == (
        "chore(book07): changes from review agent on book 7 chapter 1"
    )
    assert git(tmp_path, "show", "-s", "--format=%b", "HEAD").strip() == summary
    committed = set(
        git(tmp_path, "diff-tree", "--no-commit-id", "--name-only", "-r", "HEAD").splitlines()
    )
    assert committed == set(changed)
    assert git(tmp_path, "status", "--short") == "M  staged.txt\n M unstaged.txt\n"


@pytest.mark.asyncio
async def test_rejects_dirty_assigned_scope_before_agent_start(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    target = tmp_path / "lean" / "Book" / "Chapter01.lean"
    target.parent.mkdir(parents=True)
    target.write_text("def value := 1\n", encoding="utf-8")
    initialize(tmp_path)
    target.write_text("def value := 2\n", encoding="utf-8")

    committer = GitCommitter(tmp_path)
    await committer.prepare()

    with pytest.raises(GitCommitError, match="uncommitted files in its exclusive scope"):
        await committer.ensure_clean(chapter)


def test_agent_commit_subject_has_custom_book_fallback(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))

    assert agent_commit_subject(config.chapters[0], Stage.PROVE) == (
        "chore(book): changes from prove agent on book book chapter 1"
    )


def agent_result(summary: str) -> AgentResult:
    return AgentResult(
        succeeded=True,
        exit_code=0,
        changed=True,
        placeholders=0,
        usage=TokenUsage(),
        report={
            "changed": True,
            "complete": True,
            "summary": summary,
            "issues": [],
            "fixup_findings": [],
            "source_issues": [],
        },
    )


class EditingExecutor(CodexExecutor):
    def __init__(self, state: StateStore, summary: str) -> None:
        self.state = state
        self.summary = summary

    async def run(
        self,
        chapter: Chapter,
        stage: Stage,
        run: RunRecord,
        *,
        feedback: str = "",
        workspace_root: Path | None = None,
    ) -> AgentResult:
        del chapter, stage, feedback
        assert workspace_root is not None
        target = workspace_root / "lean" / "Book" / "Chapter01.lean"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("theorem committed : True := by trivial\n", encoding="utf-8")
        result = agent_result(self.summary)
        await self.state.finish_run(
            run,
            status=TaskStatus.SUCCEEDED,
            changed=True,
            placeholders=0,
            report=result.report,
            usage=result.usage,
        )
        return result


@pytest.mark.asyncio
async def test_orchestrator_commits_returned_agent_change_and_records_sha(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    initialize(tmp_path)
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    summary = "Added `committed` as the chapter's initial theorem."
    orchestrator.executor = EditingExecutor(state, summary)

    attempt = await orchestrator._attempt(config.chapters[0], Stage.FORMALIZE)

    assert attempt.run.isolation is not None
    commit = attempt.run.isolation["commit"]
    assert commit == git(tmp_path, "rev-parse", "HEAD").strip()
    assert git(tmp_path, "show", "-s", "--format=%b", "HEAD").strip() == summary
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_commit_failure_marks_returned_run_failed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()
    orchestrator.executor = EditingExecutor(state, "Added a theorem.")

    async def fail_commit(*_args: object, **_kwargs: object) -> str:
        raise GitCommitError("commit refused")

    monkeypatch.setattr(orchestrator.git, "commit", fail_commit)

    with pytest.raises(GitCommitError, match="commit refused"):
        await orchestrator._attempt(config.chapters[0], Stage.FORMALIZE)

    run = state.task(config.chapters[0].id, Stage.FORMALIZE).runs[-1]
    assert run.status is TaskStatus.FAILED
    assert run.isolation is not None
    assert run.isolation["accepted"] is True
    assert "integrated but orchestration failed" in run.isolation["error"]
    await orchestrator.shutdown()
