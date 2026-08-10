import asyncio
from pathlib import Path

import pytest

from lastlib_swarm.corpus import build_corpus_schedule
from lastlib_swarm.models import BookConfig
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
