from __future__ import annotations

import heapq
from dataclasses import dataclass
from typing import Literal

from lastlib_swarm.models import BookConfig, Chapter

Phase = Literal["statements", "proofs"]


@dataclass(frozen=True)
class CorpusSchedule:
    """A dependency-safe, critical-path-prioritized schedule for one corpus phase."""

    phase: Phase
    dependencies: dict[str, frozenset[str]]
    successors: dict[str, frozenset[str]]
    effort: dict[str, float]
    rank: dict[str, float]
    order: tuple[str, ...]
    critical_path: tuple[str, ...]

    def priority(self, book_id: str) -> float:
        return self.rank[book_id]


def _cycle_path(dependencies: dict[str, set[str]]) -> tuple[str, ...]:
    visiting: set[str] = set()
    visited: set[str] = set()
    path: list[str] = []

    def visit(book_id: str) -> tuple[str, ...] | None:
        visiting.add(book_id)
        path.append(book_id)
        for dependency in sorted(dependencies[book_id]):
            if dependency in visiting:
                start = path.index(dependency)
                return (*path[start:], dependency)
            if dependency not in visited:
                cycle = visit(dependency)
                if cycle is not None:
                    return cycle
        path.pop()
        visiting.remove(book_id)
        visited.add(book_id)
        return None

    for book_id in sorted(dependencies):
        if book_id not in visited:
            cycle = visit(book_id)
            if cycle is not None:
                return cycle
    return ()


def build_corpus_schedule(
    books: tuple[BookConfig, ...],
    chapters: tuple[Chapter, ...],
    *,
    phase: Phase,
    selected_books: set[str] | None = None,
) -> CorpusSchedule:
    """Compute weighted bottom-level ranks and a priority topological order.

    A book's bottom level is its own estimated effort plus the largest bottom level
    among its successors. Scheduling ready work by descending bottom level is the
    standard critical-path list-scheduling heuristic.
    """

    by_id = {book.id: book for book in books}
    selected = set(by_id) if selected_books is None else set(selected_books)
    unknown = selected - set(by_id)
    if unknown:
        raise ValueError(f"unknown selected books: {', '.join(sorted(unknown))}")

    dependencies = {book_id: set(by_id[book_id].depends_on) & selected for book_id in selected}
    successors = {book_id: set() for book_id in selected}
    for book_id, required in dependencies.items():
        for dependency in required:
            successors[dependency].add(book_id)

    indegree = {book_id: len(required) for book_id, required in dependencies.items()}
    ready = [book_id for book_id, degree in indegree.items() if degree == 0]
    heapq.heapify(ready)
    topological: list[str] = []
    while ready:
        book_id = heapq.heappop(ready)
        topological.append(book_id)
        for successor in sorted(successors[book_id]):
            indegree[successor] -= 1
            if indegree[successor] == 0:
                heapq.heappush(ready, successor)
    if len(topological) != len(selected):
        cycle = _cycle_path(dependencies)
        detail = " -> ".join(cycle) if cycle else "unknown cycle"
        raise ValueError(f"book dependency graph contains a cycle: {detail}")

    chapter_counts = {book_id: 0 for book_id in selected}
    for chapter in chapters:
        if chapter.book_id in chapter_counts:
            chapter_counts[chapter.book_id] += 1
    effort: dict[str, float] = {}
    for book_id in selected:
        configured = (
            by_id[book_id].statement_effort
            if phase == "statements"
            else by_id[book_id].proof_effort
        )
        effort[book_id] = configured if configured is not None else max(chapter_counts[book_id], 1)

    rank: dict[str, float] = {}
    for book_id in reversed(topological):
        downstream = max((rank[item] for item in successors[book_id]), default=0.0)
        rank[book_id] = effort[book_id] + downstream

    # Produce a deterministic topological order, choosing the highest bottom level
    # whenever several dependency-ready books compete.
    indegree = {book_id: len(required) for book_id, required in dependencies.items()}
    priority_ready = [
        (-rank[book_id], book_id) for book_id, degree in indegree.items() if degree == 0
    ]
    heapq.heapify(priority_ready)
    order: list[str] = []
    while priority_ready:
        _, book_id = heapq.heappop(priority_ready)
        order.append(book_id)
        for successor in successors[book_id]:
            indegree[successor] -= 1
            if indegree[successor] == 0:
                heapq.heappush(priority_ready, (-rank[successor], successor))

    roots = [book_id for book_id in selected if not dependencies[book_id]]
    critical: list[str] = []
    if roots:
        current = max(roots, key=lambda item: (rank[item], item))
        while True:
            critical.append(current)
            if not successors[current]:
                break
            current = max(successors[current], key=lambda item: (rank[item], item))

    return CorpusSchedule(
        phase=phase,
        dependencies={key: frozenset(value) for key, value in dependencies.items()},
        successors={key: frozenset(value) for key, value in successors.items()},
        effort=effort,
        rank=rank,
        order=tuple(order),
        critical_path=tuple(critical),
    )
