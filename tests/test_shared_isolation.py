from pathlib import Path

import pytest

from lastlib_swarm.config import load_config
from lastlib_swarm.isolation import SharedIsolation
from tests.support import write_project


@pytest.mark.asyncio
async def test_shared_workspace_reports_exact_created_modified_and_deleted_paths(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    chapter_root = tmp_path / "lean" / "Book" / "Chapter01"
    chapter_root.mkdir(parents=True)
    modified = chapter_root / "Modified.lean"
    deleted = chapter_root / "Deleted.lean"
    modified.write_text("def value := 1\n", encoding="utf-8")
    deleted.write_text("def obsolete := 1\n", encoding="utf-8")

    workspace = await SharedIsolation(config.settings).acquire("shared-agent")
    await workspace.snapshot(chapter)
    modified.write_text("def value := 2\n", encoding="utf-8")
    deleted.unlink()
    created = chapter_root / "Created.lean"
    created.write_text("def fresh := 1\n", encoding="utf-8")

    result = await workspace.collect(chapter)

    assert result.accepted
    assert result.changed_paths == tuple(
        sorted(path.relative_to(tmp_path).as_posix() for path in (created, deleted, modified))
    )
