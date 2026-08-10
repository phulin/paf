from pathlib import Path

import pytest

from lastlib_swarm.config import infer_config, load_config
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


def test_infers_zero_config_project_from_markdown(tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()
    books = tmp_path / "books"
    books.mkdir()
    target = books / "07-example-theory.md"
    target.write_text(
        "# Example Theory\n\n## 1. Foundations\n\n## 2. Main result\n", encoding="utf-8"
    )
    existing = tmp_path / "lean" / "LastLib" / "Book07ExistingAPI"
    existing.mkdir(parents=True)

    config = infer_config(target)

    assert config.settings.repo == tmp_path
    assert config.settings.model == "gpt-5.6-luna"
    assert config.settings.reasoning_effort == "max"
    assert config.settings.state_dir == tmp_path / ".swarm" / "book07"
    assert config.books[0].module == "LastLib.Book07ExistingAPI"
    assert [chapter.number for chapter in config.chapters] == [1, 2]
    assert all(stage.prompt.is_file() for stage in config.stages.values())


def test_config_stage_prompts_are_optional(tmp_path: Path) -> None:
    path = write_project(tmp_path)
    text = path.read_text(encoding="utf-8")
    start = text.index("[stages.formalize]")
    end = text.index("[[books]]")
    path.write_text(text[:start] + text[end:], encoding="utf-8")

    config = load_config(path)

    assert config.stages[Stage.FORMALIZE].prompt.name == "formalize.md"
