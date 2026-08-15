import shutil
from pathlib import Path

import pytest

from paf.config import load_config
from paf.models import SourceDocument, SourceSpan, Stage, WorkUnit
from paf.state import StateStore, TaskStatus
from tests.support import write_project


def test_legacy_markdown_adapts_to_documents_and_stable_work_units(tmp_path: Path) -> None:
    project = tmp_path / "original" / "project"
    project.mkdir(parents=True)
    config = load_config(write_project(project))

    assert config.documents == (
        SourceDocument(
            id="book",
            path=Path("books/book.md"),
            format="markdown",
            title="A Book",
            metadata={
                "profile": "numbered-chapters",
                "heading_pattern": config.books[0].heading_pattern,
            },
        ),
    )
    assert all(isinstance(unit, WorkUnit) for unit in config.work_units)
    assert [(unit.id, unit.ordinal, unit.source_span) for unit in config.work_units] == [
        ("book/chapter-01", 1, SourceSpan(3, 6)),
        ("book/chapter-02", 2, SourceSpan(7, 7)),
    ]
    assert [unit.id for unit in config.work_units] == [chapter.id for chapter in config.chapters]


@pytest.mark.asyncio
async def test_work_unit_and_persisted_task_ids_survive_project_move(tmp_path: Path) -> None:
    original = tmp_path / "one" / "project"
    original.mkdir(parents=True)
    original_config = load_config(write_project(original, chapters="chapters = [1]"))
    original_unit = original_config.work_units[0]
    state = StateStore(original_config)
    await state.load_or_create()
    run = await state.start_run(original_unit.id, Stage.FORMALIZE)
    await state.finish_run(run, status=TaskStatus.SUCCEEDED)
    await state.set_task(original_unit.id, Stage.FORMALIZE, TaskStatus.SUCCEEDED, "done")
    original_keys = tuple(state.hot_snapshot()["tasks"])

    moved = tmp_path / "two" / "renamed-project"
    moved.parent.mkdir(parents=True)
    shutil.move(original, moved)
    moved_config = load_config(moved / "paf.toml")
    moved_state = StateStore(moved_config)
    await moved_state.load_or_create()

    moved_unit = moved_config.work_units[0]
    assert moved_unit.id == original_unit.id == "book/chapter-01"
    assert moved_unit.document.path == original_unit.document.path == Path("books/book.md")
    assert tuple(moved_state.hot_snapshot()["tasks"]) == original_keys
    task_payload = moved_state.hot_snapshot()["tasks"]["book/chapter-01:formalize"]
    assert task_payload["work_unit_id"] == "book/chapter-01"
    assert task_payload["document_id"] == "book"
    assert task_payload["ordinal"] == 1
    assert task_payload["unit_title"] == "First chapter"
    assert moved_state.task(moved_unit.id, Stage.FORMALIZE).status == TaskStatus.SUCCEEDED
