from __future__ import annotations

import heapq
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from paf.models import SourceDocumentLike, WorkUnitLike
from paf.scope import ScopeMatcher

Phase = Literal["statements", "proofs"]

LEAN_IMPORT_RE = re.compile(
    r"^[ \t]*(?:public[ \t]+)?import[ \t]+(?P<modules>[^\r\n]+)",
    re.MULTILINE,
)
LEAN_MODULE_RE = re.compile(r"\bLastLib(?:\.[A-Za-z0-9_']+)+\b")


@dataclass(frozen=True)
class WorkUnitImportGraph:
    """The current cross-unit graph observed directly in target imports."""

    dependencies: dict[str, frozenset[str]]
    successors: dict[str, frozenset[str]]
    order: tuple[str, ...]
    edges: tuple[tuple[str, str], ...]

    def snapshot(self) -> dict[str, object]:
        return {
            "algorithm": "observed-lean-imports",
            "order": list(self.order),
            "edges": [list(edge) for edge in self.edges],
            "dependencies": {
                work_unit_id: sorted(required)
                for work_unit_id, required in sorted(self.dependencies.items())
            },
        }


def observed_imports(text: str) -> tuple[str, ...]:
    """Extract LastLib modules from ordinary and public Lean import lines."""

    modules: dict[str, None] = {}
    for match in LEAN_IMPORT_RE.finditer(text):
        imported = match.group("modules").split("--", 1)[0]
        for module in LEAN_MODULE_RE.findall(imported):
            modules[module] = None
    return tuple(modules)


def _work_unit_target_files(repo: Path, work_unit: WorkUnitLike) -> tuple[Path, ...]:
    return tuple(ScopeMatcher(work_unit.scope).files(repo))


def _chapter_cycle(dependencies: dict[str, set[str]]) -> tuple[str, ...]:
    visiting: set[str] = set()
    visited: set[str] = set()
    path: list[str] = []

    def visit(chapter_id: str) -> tuple[str, ...] | None:
        visiting.add(chapter_id)
        path.append(chapter_id)
        for dependency in sorted(dependencies[chapter_id]):
            if dependency in visiting:
                start = path.index(dependency)
                return (*path[start:], dependency)
            if dependency not in visited:
                cycle = visit(dependency)
                if cycle is not None:
                    return cycle
        path.pop()
        visiting.remove(chapter_id)
        visited.add(chapter_id)
        return None

    for chapter_id in sorted(dependencies):
        if chapter_id not in visited and (cycle := visit(chapter_id)):
            return cycle
    return ()


def build_work_unit_import_graph(
    repo: Path, work_units: tuple[WorkUnitLike, ...]
) -> WorkUnitImportGraph:
    """Build a deterministic work-unit DAG from currently observed imports."""

    by_id = {work_unit.id: work_unit for work_unit in work_units}
    dependencies = {work_unit.id: set(work_unit.depends_on) for work_unit in work_units}
    missing = {
        dependency
        for required in dependencies.values()
        for dependency in required
        if dependency not in by_id
    }
    if missing:
        raise ValueError(f"work units depend on unknown ids: {', '.join(sorted(missing))}")
    module_owners = sorted(
        ((work_unit.chapter_module, work_unit.id) for work_unit in work_units),
        key=lambda item: len(item[0]),
        reverse=True,
    )

    def owner(module: str) -> str | None:
        for prefix, chapter_id in module_owners:
            if module == prefix or module.startswith(prefix + "."):
                return chapter_id
        return None

    for dependent in work_units:
        for path in _work_unit_target_files(repo, dependent):
            for module in observed_imports(path.read_text(encoding="utf-8")):
                prerequisite = owner(module)
                if prerequisite is not None and prerequisite != dependent.id:
                    dependencies[dependent.id].add(prerequisite)

    successors = {work_unit.id: set() for work_unit in work_units}
    for dependent, required in dependencies.items():
        for prerequisite in required:
            successors[prerequisite].add(dependent)

    indegree = {chapter_id: len(required) for chapter_id, required in dependencies.items()}
    ready = [chapter_id for chapter_id, degree in indegree.items() if degree == 0]
    heapq.heapify(ready)
    order: list[str] = []
    while ready:
        chapter_id = heapq.heappop(ready)
        order.append(chapter_id)
        for successor in sorted(successors[chapter_id]):
            indegree[successor] -= 1
            if indegree[successor] == 0:
                heapq.heappush(ready, successor)

    if len(order) != len(by_id):
        cycle = _chapter_cycle(dependencies)
        detail = " -> ".join(cycle) if cycle else "unknown cycle"
        raise ValueError(f"observed chapter import graph contains a cycle: {detail}")

    edges = tuple(
        sorted(
            (prerequisite, dependent)
            for dependent, required in dependencies.items()
            for prerequisite in required
        )
    )
    return WorkUnitImportGraph(
        dependencies={key: frozenset(value) for key, value in dependencies.items()},
        successors={key: frozenset(value) for key, value in successors.items()},
        order=tuple(order),
        edges=edges,
    )


# Compatibility names for callers that still construct ``Chapter`` adapters.
ChapterImportGraph = WorkUnitImportGraph
build_chapter_import_graph = build_work_unit_import_graph


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

    def priority(self, document_id: str) -> float:
        return self.rank[document_id]

    def snapshot(self) -> dict[str, object]:
        return {
            "order": list(self.order),
            "critical_path": list(self.critical_path),
            "rank": self.rank,
            "effort": self.effort,
        }


def scheduling_snapshot(statements: CorpusSchedule, proofs: CorpusSchedule) -> dict[str, object]:
    return {
        "algorithm": "weighted-critical-path-list-scheduling",
        "statements": statements.snapshot(),
        "proofs": proofs.snapshot(),
    }


def scheduling_summary(snapshot: dict[str, object]) -> dict[str, object]:
    summary: dict[str, object] = {"algorithm": snapshot.get("algorithm", "unknown")}
    for phase in ("statements", "proofs"):
        value = snapshot.get(phase)
        if isinstance(value, dict):
            summary[phase] = {"critical_path": value.get("critical_path", [])}
    return summary


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
    documents: tuple[SourceDocumentLike, ...],
    work_units: tuple[WorkUnitLike, ...],
    *,
    phase: Phase,
    selected_documents: set[str] | None = None,
    selected_books: set[str] | None = None,
) -> CorpusSchedule:
    """Compute weighted bottom-level ranks and a priority topological order.

    A book's bottom level is its own estimated effort plus the largest bottom level
    among its successors. Scheduling ready work by descending bottom level is the
    standard critical-path list-scheduling heuristic.
    """

    if selected_documents is not None and selected_books is not None:
        raise ValueError("pass selected_documents or legacy selected_books, not both")
    selected_ids = selected_documents if selected_documents is not None else selected_books
    by_id = {document.id: document for document in documents}
    selected = set(by_id) if selected_ids is None else set(selected_ids)
    unknown = selected - set(by_id)
    if unknown:
        raise ValueError(f"unknown selected documents: {', '.join(sorted(unknown))}")

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
    for work_unit in work_units:
        if work_unit.document_id in chapter_counts:
            chapter_counts[work_unit.document_id] += 1
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
