from __future__ import annotations

import re
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from paf.scope import ScopeMatcher


@dataclass(frozen=True)
class WarningDiagnostic:
    path: str
    line: int
    column: int
    message: str
    text: str


@dataclass(frozen=True)
class WarningCleanupResult:
    applied: bool
    changed_paths: tuple[str, ...] = ()
    warning_count: int = 0
    reason: str = ""
    rewrites: tuple[WarningSourceRewrite, ...] = ()


@dataclass(frozen=True)
class WarningSourceRewrite:
    relative_path: str
    original: str
    updated: str


@dataclass(frozen=True)
class _Edit:
    start: int
    end: int
    replacement: str


_ATOMIC_SIMP_ARGUMENT_RE = re.compile(r"(?:←\s*)?[\w'.]+", re.UNICODE)
_SIMP_LIST_RE = re.compile(r"\bsimp(?:\s+only)?\s*\[(?P<arguments>[^\[\]]*)\]")
_SIMP_AT_RE = re.compile(
    r"Try `simp at (?P<name>[\w'.]+)` instead of `simpa using (?P=name)`$",
    re.UNICODE,
)


def _source_offset(line: str, column: int, expected: str) -> int | None:
    """Translate a Lean column conservatively across ASCII and UTF-8 conventions."""

    candidates = {column}
    with suppress(UnicodeDecodeError):
        candidates.add(len(line.encode("utf-8")[:column].decode("utf-8")))
    matches = {
        candidate
        for candidate in candidates
        if 0 <= candidate <= len(line) and line.startswith(expected, candidate)
    }
    return matches.pop() if len(matches) == 1 else None


def _token_edit(
    line: str,
    column: int,
    *,
    before: str,
    after: str,
) -> _Edit | None:
    offset = _source_offset(line, column, before)
    if offset is None:
        return None
    if before[0].isalnum() or before[0] == "_":
        left = line[offset - 1] if offset else ""
        right = line[offset + len(before)] if offset + len(before) < len(line) else ""
        if (left.isalnum() or left == "_") or (right.isalnum() or right == "_"):
            return None
    return _Edit(offset, offset + len(before), after)


def _unused_simp_argument(diagnostic: WarningDiagnostic, line: str) -> _Edit | None:
    body = diagnostic.text.splitlines()
    try:
        header_index = next(
            index for index, value in enumerate(body) if value.strip() == diagnostic.message
        )
    except StopIteration:
        header_index = 0
    argument = next(
        (value.strip() for value in body[header_index + 1 :] if value.strip()),
        "",
    )
    if not argument or _ATOMIC_SIMP_ARGUMENT_RE.fullmatch(argument) is None:
        return None
    argument_offset = _source_offset(line, diagnostic.column, argument)
    if argument_offset is None:
        return None

    containing: list[tuple[re.Match[str], list[tuple[int, int, str]]]] = []
    for match in _SIMP_LIST_RE.finditer(line):
        arguments_start = match.start("arguments")
        arguments: list[tuple[int, int, str]] = []
        cursor = 0
        for item in match.group("arguments").split(","):
            stripped = item.strip()
            leading = len(item) - len(item.lstrip())
            start = arguments_start + cursor + leading
            end = start + len(stripped)
            arguments.append((start, end, stripped))
            cursor += len(item) + 1
        if any(start == argument_offset for start, _end, _value in arguments):
            containing.append((match, arguments))
    if len(containing) != 1:
        return None
    _match, arguments = containing[0]
    if len(arguments) < 2 or any(
        not value or _ATOMIC_SIMP_ARGUMENT_RE.fullmatch(value) is None
        for _start, _end, value in arguments
    ):
        return None
    indexes = [
        index
        for index, (start, _end, value) in enumerate(arguments)
        if start == argument_offset and value == argument
    ]
    if len(indexes) != 1:
        return None
    index = indexes[0]
    if index + 1 < len(arguments):
        return _Edit(arguments[index][0], arguments[index + 1][0], "")
    return _Edit(arguments[index - 1][1], arguments[index][1], "")


def _diagnostic_edit(diagnostic: WarningDiagnostic, line: str) -> _Edit | None:
    message = diagnostic.message
    if message == "try 'simp' instead of 'simpa'":
        return _token_edit(line, diagnostic.column, before="simpa", after="simp")
    if message == "`push_neg` has been deprecated. Prefer using `push Not` instead.":
        return _token_edit(line, diagnostic.column, before="push_neg", after="push Not")
    if message == "Used `tac1 <;> tac2` where `(tac1; tac2)` would suffice":
        return _token_edit(line, diagnostic.column, before="<;>", after=";")
    if match := _SIMP_AT_RE.fullmatch(message):
        name = match.group("name")
        return _token_edit(
            line,
            diagnostic.column,
            before=f"simpa using {name}",
            after=f"simp at {name}",
        )
    if message == "Try this:":
        if "so `let` is preferred over `letI`." in diagnostic.text:
            return _token_edit(line, diagnostic.column, before="letI", after="let")
        if "so `have` is preferred over `haveI`." in diagnostic.text:
            return _token_edit(line, diagnostic.column, before="haveI", after="have")
        return None
    if message == "This simp argument is unused:":
        return _unused_simp_argument(diagnostic, line)
    return None


def apply_deterministic_warning_cleanup(
    *,
    repo_root: Path,
    lean_root: Path,
    scope: tuple[str, ...],
    diagnostics: tuple[WarningDiagnostic, ...],
) -> WarningCleanupResult:
    """Apply an all-or-nothing allowlist of location-bound warning edits."""

    if not diagnostics:
        return WarningCleanupResult(False, reason="no warning diagnostics were supplied")
    matcher = ScopeMatcher(scope)
    sources: dict[Path, str] = {}
    edits: dict[Path, list[_Edit]] = {}
    relative_paths: dict[Path, str] = {}

    for diagnostic in diagnostics:
        diagnostic_path = PurePosixPath(diagnostic.path)
        if diagnostic_path.is_absolute() or ".." in diagnostic_path.parts:
            return WarningCleanupResult(False, reason=f"unsafe warning path: {diagnostic.path}")
        path = lean_root.joinpath(*diagnostic_path.parts)
        try:
            relative = path.relative_to(repo_root).as_posix()
        except ValueError:
            return WarningCleanupResult(False, reason=f"warning path escaped repository: {path}")
        if not matcher.matches(relative):
            return WarningCleanupResult(False, reason=f"warning path is outside scope: {relative}")
        try:
            source = sources.setdefault(path, path.read_text(encoding="utf-8"))
        except OSError as error:
            return WarningCleanupResult(False, reason=f"could not read {relative}: {error}")
        lines = source.splitlines(keepends=True)
        if diagnostic.line < 1 or diagnostic.line > len(lines):
            return WarningCleanupResult(False, reason=f"stale warning line in {relative}")
        line = lines[diagnostic.line - 1].removesuffix("\n").removesuffix("\r")
        edit = _diagnostic_edit(diagnostic, line)
        if edit is None:
            return WarningCleanupResult(
                False,
                reason=(
                    f"warning did not match the deterministic allowlist at "
                    f"{relative}:{diagnostic.line}:{diagnostic.column}"
                ),
            )
        line_start = sum(len(value) for value in lines[: diagnostic.line - 1])
        absolute = _Edit(line_start + edit.start, line_start + edit.end, edit.replacement)
        edits.setdefault(path, []).append(absolute)
        relative_paths[path] = relative

    updated: dict[Path, str] = {}
    for path, path_edits in edits.items():
        source = sources[path]
        ordered = sorted(path_edits, key=lambda edit: (edit.start, edit.end), reverse=True)
        previous_start = len(source) + 1
        for edit in ordered:
            if edit.end > previous_start:
                return WarningCleanupResult(
                    False, reason=f"overlapping edits in {relative_paths[path]}"
                )
            source = source[: edit.start] + edit.replacement + source[edit.end :]
            previous_start = edit.start
        updated[path] = source

    try:
        for path, source in updated.items():
            path.write_text(source, encoding="utf-8")
    except OSError as error:
        return WarningCleanupResult(False, reason=f"could not write deterministic cleanup: {error}")
    return WarningCleanupResult(
        True,
        changed_paths=tuple(sorted(relative_paths.values())),
        warning_count=len(diagnostics),
        rewrites=tuple(
            WarningSourceRewrite(relative_paths[path], sources[path], updated[path])
            for path in sorted(updated, key=relative_paths.__getitem__)
        ),
    )


def revert_deterministic_warning_cleanup(
    *,
    repo_root: Path,
    rewrites: tuple[WarningSourceRewrite, ...],
) -> tuple[str, ...]:
    """Restore a cleanup only when every source still equals its deterministic result."""

    current: dict[Path, str] = {}
    for rewrite in rewrites:
        path = repo_root / rewrite.relative_path
        value = path.read_text(encoding="utf-8")
        if value != rewrite.updated:
            raise RuntimeError(
                f"cannot revert deterministic warning cleanup after {rewrite.relative_path} changed"
            )
        current[path] = rewrite.original
    for path, source in current.items():
        path.write_text(source, encoding="utf-8")
    return tuple(rewrite.relative_path for rewrite in rewrites)
