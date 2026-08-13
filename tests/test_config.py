from pathlib import Path

import pytest

from lastlib_swarm.config import (
    infer_config,
    infer_corpus,
    load_config,
    parse_book_dependencies,
)
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
    assert config.stages[Stage.FORMALIZE].max_rounds == 1
    assert config.stages[Stage.FIXUP].max_rounds == 10
    assert config.stages[Stage.REVIEW].max_rounds == 5
    assert config.settings.state_dir == tmp_path / ".swarm"
    assert config.settings.lean_mcp
    assert config.settings.lean_project == Path("lean")
    assert config.settings.lean_mcp_tool_timeout_seconds == 300
    assert config.settings.capacity_resume_attempts == 10
    assert config.settings.capacity_resume_delay_seconds == 15
    assert config.settings.capacity_resume_max_delay_seconds == 120
    assert config.settings.sandbox == "danger-full-access"
    assert config.settings.cache_compaction_layers == 32


def test_selects_configured_chapters(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [2]"))

    assert [chapter.number for chapter in config.chapters] == [2]


def test_loads_phase_specific_book_effort(tmp_path: Path) -> None:
    path = write_project(tmp_path)
    text = path.read_text(encoding="utf-8").replace(
        'module = "Book"',
        'module = "Book"\nstatement_effort = 2.5\nproof_effort = 9',
    )
    path.write_text(text, encoding="utf-8")

    book = load_config(path).books[0]

    assert book.statement_effort == 2.5
    assert book.proof_effort == 9


def test_rejects_unknown_isolation_backend(tmp_path: Path) -> None:
    path = write_project(tmp_path)
    path.write_text(
        path.read_text(encoding="utf-8").replace('isolation = "shared"', 'isolation = "telepathy"'),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"swarm\.isolation"):
        load_config(path)


def test_loads_and_validates_cache_compaction_threshold(tmp_path: Path) -> None:
    path = write_project(tmp_path)
    path.write_text(
        path.read_text(encoding="utf-8").replace(
            'isolation = "shared"',
            'isolation = "shared"\ncache_compaction_layers = 8',
        ),
        encoding="utf-8",
    )

    assert load_config(path).settings.cache_compaction_layers == 8

    path.write_text(
        path.read_text(encoding="utf-8").replace(
            "cache_compaction_layers = 8", "cache_compaction_layers = 1"
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match=r"swarm\.cache_compaction_layers"):
        load_config(path)


def test_loads_lean_mcp_settings(tmp_path: Path) -> None:
    path = write_project(tmp_path)
    path.write_text(
        path.read_text(encoding="utf-8").replace(
            'isolation = "shared"',
            'isolation = "shared"\nlean_mcp = false\nlean_project = "lean-project"\n'
            "lean_mcp_tool_timeout_seconds = 45",
        ),
        encoding="utf-8",
    )

    settings = load_config(path).settings

    assert not settings.lean_mcp
    assert settings.lean_project == Path("lean-project")
    assert settings.lean_mcp_tool_timeout_seconds == 45


def test_rejects_lean_project_outside_repository(tmp_path: Path) -> None:
    path = write_project(tmp_path)
    path.write_text(
        path.read_text(encoding="utf-8").replace(
            'isolation = "shared"', 'isolation = "shared"\nlean_project = "../outside"'
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"swarm\.lean_project"):
        load_config(path)


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
    assert config.settings.bypass_approvals_and_sandbox
    assert config.settings.isolation == "auto"
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


def test_legacy_repair_stage_config_maps_to_fixup(tmp_path: Path) -> None:
    path = write_project(tmp_path)
    text = path.read_text(encoding="utf-8").replace("[stages.fixup]", "[stages.repair]")
    path.write_text(text, encoding="utf-8")

    config = load_config(path)

    assert config.stages[Stage.FIXUP].prompt.name == "fixup.md"


def test_infers_multiple_books_and_chained_mermaid_dependencies(tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()
    books = tmp_path / "books"
    books.mkdir()
    for number in range(1, 4):
        (books / f"{number:02d}-book.md").write_text(
            f"# Book {number}\n\n## 1. Chapter\n", encoding="utf-8"
        )
    graph = tmp_path / "BOOK_DEPENDENCIES.md"
    graph.write_text("B01 --> B02 --> B03\n", encoding="utf-8")

    assert parse_book_dependencies(graph) == {
        "book02": ("book01",),
        "book03": ("book02",),
    }
    config = infer_corpus((books,))

    assert [book.id for book in config.books] == ["book01", "book02", "book03"]
    assert config.books[2].depends_on == ("book02",)
    assert config.settings.state_dir.parent == tmp_path / ".swarm"
