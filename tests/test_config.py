from pathlib import Path

import pytest

from lastlib_swarm.config import load_config
from lastlib_swarm.models import Stage
from tests.support import write_project


def test_discovers_chapters_and_renders_paths(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path))

    assert [chapter.id for chapter in config.chapters] == [
        "book/chapter-01",
        "book/chapter-02",
    ]
    first = config.chapters[0]
    assert first.chapter_module == "Book.Chapter01"
    assert first.scope == ("lean/Book/Chapter01.lean", "lean/Book/Chapter01/**/*.lean")
    assert config.stages[Stage.REVIEW].max_rounds == 5
    assert config.settings.state_dir == tmp_path / ".swarm"


def test_selects_configured_chapters(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [2]"))

    assert [chapter.number for chapter in config.chapters] == [2]


def test_rejects_unknown_book_dependency(tmp_path: Path) -> None:
    path = write_project(tmp_path)
    text = path.read_text(encoding="utf-8").replace(
        'module = "Book"', 'module = "Book"\ndepends_on = ["missing"]'
    )
    path.write_text(text, encoding="utf-8")

    with pytest.raises(ValueError, match="unconfigured books"):
        load_config(path)
