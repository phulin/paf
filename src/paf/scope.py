from __future__ import annotations

from dataclasses import dataclass
from functools import cache
from pathlib import Path, PurePosixPath


@cache
def _globstar_variants(pattern: str) -> tuple[str, ...]:
    """Expand `**/` to the zero-directory spellings accepted by pathlib glob."""

    variants = {pattern}
    pending = [pattern]
    while pending:
        candidate = pending.pop()
        start = 0
        while (index := candidate.find("**/", start)) >= 0:
            collapsed = candidate[:index] + candidate[index + 3 :]
            if collapsed not in variants:
                variants.add(collapsed)
                pending.append(collapsed)
            start = index + 3
    return tuple(sorted(variants))


@dataclass(frozen=True)
class ScopeMatcher:
    """One anchored pathlib-style matcher shared by discovery and acceptance."""

    patterns: tuple[str, ...]

    def matches(self, relative: str | Path) -> bool:
        normalized = Path(relative).as_posix().removeprefix("./")
        candidate = PurePosixPath("/" + normalized)
        return any(
            candidate.match("/" + variant)
            for pattern in self.patterns
            for variant in _globstar_variants(pattern)
        )

    def files(self, root: Path) -> list[Path]:
        candidates = {
            path
            for pattern in self.patterns
            for path in root.glob(pattern)
            if path.is_file() and self.matches(path.relative_to(root))
        }
        return sorted(candidates)

    def has_match_for_each_pattern(self, root: Path) -> bool:
        return all(
            any(
                path.is_file() and ScopeMatcher((pattern,)).matches(path.relative_to(root))
                for path in root.glob(pattern)
            )
            for pattern in self.patterns
        )
