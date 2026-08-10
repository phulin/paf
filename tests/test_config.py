from pathlib import Path

import pytest

from lastlib_swarm.config import load_config
from lastlib_swarm.models import Stage


def write_project(tmp_path: Path, *, chapters: str = "") -> Path:
    (tmp_path / "books").mkdir()
    (tmp_path / "prompts").mkdir()
    (tmp_path / "books" / "book.md").write_text(
        "# Book\n\n## 1. First chapter\n\nText.\n\n## 2. Second chapter\n",
        encoding="utf-8",
    )
    for stage in Stage:
        (tmp_path / "prompts" / f"{stage}.md").write_text(
            "Do {book_title} chapter {chapter_number_padded}", encoding="utf-8"
        )
    config = tmp_path / "swarm.toml"
    config.write_text(
        f"""
[swarm]
repo = "."
max_agents = 4

[stages.formalize]
prompt = "prompts/formalize.md"
[stages.review]
prompt = "prompts/review.md"
[stages.prove]
prompt = "prompts/prove.md"
[stages.repair]
prompt = "prompts/repair.md"

[[books]]
id = "book"
title = "A Book"
source = "books/book.md"
lean_root = "lean/Book"
module = "Book"
{chapters}
""",
        encoding="utf-8",
    )
    return config


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
