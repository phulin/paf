import asyncio
from pathlib import Path

import pytest

from lastlib_swarm.corpus import (
    build_chapter_import_graph,
    build_corpus_schedule,
    observed_imports,
)
from lastlib_swarm.models import BookConfig, Chapter
from lastlib_swarm.scheduler import PriorityLimiter


def book(
    book_id: str,
    *,
    depends_on: tuple[str, ...] = (),
    effort: float = 1,
) -> BookConfig:
    return BookConfig(
        id=book_id,
        title=book_id,
        source=Path(f"{book_id}.md"),
        lean_root=Path("lean") / book_id,
        module=book_id,
        depends_on=depends_on,
        statement_effort=effort,
        proof_effort=effort,
    )


def chapter(number: int) -> Chapter:
    module = f"LastLib.Book.Chapter{number:02d}"
    return Chapter(
        book_id="book",
        book_title="Book",
        number=number,
        title=f"Chapter {number}",
        source=Path("book.md"),
        lean_root=Path("lean/LastLib/Book"),
        module="LastLib.Book",
        chapter_path=f"Chapter{number:02d}",
        chapter_module=module,
        build_command=f"lake build +{module}",
        scope=(
            f"lean/LastLib/Book/Chapter{number:02d}.lean",
            f"lean/LastLib/Book/Chapter{number:02d}/**/*.lean",
        ),
        depends_on_books=(),
    )


def test_bottom_level_ranks_prioritize_the_weighted_critical_path() -> None:
    books = (
        book("foundation", effort=2),
        book("short-independent", effort=8),
        book("middle", depends_on=("foundation",), effort=4),
        book("capstone", depends_on=("middle",), effort=6),
    )

    schedule = build_corpus_schedule(books, (), phase="statements")

    assert schedule.rank == {
        "foundation": 12,
        "short-independent": 8,
        "middle": 10,
        "capstone": 6,
    }
    assert schedule.critical_path == ("foundation", "middle", "capstone")
    assert schedule.order == (
        "foundation",
        "middle",
        "short-independent",
        "capstone",
    )


def test_dependency_cycle_is_reported_with_its_path() -> None:
    books = (
        book("a", depends_on=("c",)),
        book("b", depends_on=("a",)),
        book("c", depends_on=("b",)),
    )

    with pytest.raises(ValueError, match=r"a -> c -> b -> a"):
        build_corpus_schedule(books, (), phase="proofs")


def test_observed_imports_uses_regexes_for_ordinary_and_public_imports() -> None:
    text = """import Mathlib.Data.Nat LastLib.Book.Chapter01.Core -- keep this
  public   import LastLib.Book.Chapter02.Section LastLib.Other.API
-- import LastLib.Commented.Out
def import_is_not_a_header := "LastLib.Book.Chapter03"
"""

    assert observed_imports(text) == (
        "LastLib.Book.Chapter01.Core",
        "LastLib.Book.Chapter02.Section",
        "LastLib.Other.API",
    )


def test_chapter_import_graph_contains_only_observed_cross_chapter_edges(
    tmp_path: Path,
) -> None:
    chapters = tuple(chapter(number) for number in (1, 2, 3))
    root = tmp_path / "lean" / "LastLib" / "Book"
    for number in (1, 2, 3):
        (root / f"Chapter{number:02d}").mkdir(parents=True)
    (root / "Chapter01.lean").write_text("import LastLib.Book.Chapter01.Core\n", encoding="utf-8")
    (root / "Chapter01" / "Core.lean").write_text("def one := 1\n", encoding="utf-8")
    (root / "Chapter02.lean").write_text(
        "import LastLib.Book.Chapter01\nimport Mathlib.Data.Nat\n", encoding="utf-8"
    )
    (root / "Chapter03.lean").write_text(
        "public import LastLib.Book.Chapter02 LastLib.Unknown\n", encoding="utf-8"
    )

    graph = build_chapter_import_graph(tmp_path, chapters)

    assert graph.edges == (
        ("book/chapter-01", "book/chapter-02"),
        ("book/chapter-02", "book/chapter-03"),
    )
    assert graph.order == (
        "book/chapter-01",
        "book/chapter-02",
        "book/chapter-03",
    )


def test_chapter_import_graph_reports_observed_cycle(tmp_path: Path) -> None:
    chapters = tuple(chapter(number) for number in (1, 2))
    root = tmp_path / "lean" / "LastLib" / "Book"
    root.mkdir(parents=True)
    (root / "Chapter01.lean").write_text("import LastLib.Book.Chapter02\n", encoding="utf-8")
    (root / "Chapter02.lean").write_text("import LastLib.Book.Chapter01\n", encoding="utf-8")

    with pytest.raises(
        ValueError,
        match=r"book/chapter-01 -> book/chapter-02 -> book/chapter-01",
    ):
        build_chapter_import_graph(tmp_path, chapters)


@pytest.mark.asyncio
async def test_priority_limiter_grants_a_contended_slot_by_rank() -> None:
    limiter = PriorityLimiter(1)
    order: list[str] = []
    await limiter.acquire(0)

    async def work(name: str, priority: float) -> None:
        async with limiter.slot(priority):
            order.append(name)
            await asyncio.sleep(0)

    low = asyncio.create_task(work("low", 1))
    high = asyncio.create_task(work("high", 100))
    await asyncio.sleep(0)
    limiter.release()
    await asyncio.gather(low, high)

    assert order == ["high", "low"]
