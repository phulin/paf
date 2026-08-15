from __future__ import annotations

import re
from collections.abc import Iterable
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from paf.models import SourceDocument, SourceSpan, WorkUnit


@runtime_checkable
class SourceAdapter(Protocol):
    """Parser boundary for format-neutral informal source files."""

    def supports(self, path: str | Path) -> bool: ...

    def read_document(self, path: str | Path) -> SourceDocument: ...

    def discover_units(self, document: SourceDocument) -> tuple[WorkUnit, ...]: ...


def _slug(value: str) -> str:
    value = re.sub(r"[^\w./-]+", "-", value.casefold(), flags=re.UNICODE)
    value = re.sub(r"-+", "-", value).strip("-./")
    return value or "source"


def _line_count(text: str) -> int:
    return max(len(text.splitlines()), 1)


class _FileAdapter:
    suffixes: frozenset[str] = frozenset()
    format: str

    def __init__(self, *, root: str | Path | None = None) -> None:
        self.root = Path(root).resolve() if root is not None else None
        self._texts: dict[Path, str] = {}

    def supports(self, path: str | Path) -> bool:
        return Path(path).suffix.casefold() in self.suffixes

    def _resolve(self, path: str | Path) -> tuple[Path, Path]:
        supplied = Path(path)
        absolute = supplied.resolve()
        if self.root is None:
            # A relative input remains relative to the caller's current project;
            # an absolute input gets a checkout-independent basename by default.
            relative = supplied if not supplied.is_absolute() else Path(supplied.name)
        else:
            try:
                relative = absolute.relative_to(self.root)
            except ValueError as error:
                raise ValueError(f"source must be inside adapter root: {absolute}") from error
        return absolute, Path(relative.as_posix())

    def _document(
        self, path: str | Path, *, title: str, metadata: dict[str, object]
    ) -> SourceDocument:
        absolute, relative = self._resolve(path)
        try:
            text = absolute.read_text(encoding="utf-8")
        except OSError as error:
            raise ValueError(f"cannot read {self.format} source: {absolute}") from error
        self._texts[relative] = text
        return SourceDocument(
            id=_slug(relative.with_suffix("").as_posix()),
            path=relative,
            format=self.format,
            title=title,
            metadata=metadata,
        )

    def _text(self, document: SourceDocument) -> str:
        try:
            return self._texts[document.path]
        except KeyError as error:
            raise ValueError("document was not read by this adapter instance") from error


class MarkdownAdapter(_FileAdapter):
    """Discover Markdown units from ATX headings.

    The default boundary is level-two ATX headings. ``numbered-chapters`` is
    the legacy profile and keeps persisted ``book/chapter-NN`` identifiers.
    """

    suffixes = frozenset({".md", ".markdown"})
    format = "markdown"
    NUMBERED_CHAPTER_PATTERN = r"^##\s+(?P<number>\d+)\.\s+(?P<title>.+?)\s*$"

    def __init__(
        self,
        *,
        root: str | Path | None = None,
        profile: str = "atx",
        heading_levels: Iterable[int] = (2,),
        heading_pattern: str | None = None,
        document_id: str | None = None,
        document_title: str | None = None,
    ) -> None:
        super().__init__(root=root)
        if profile not in {"atx", "numbered-chapters"}:
            raise ValueError("Markdown profile must be 'atx' or 'numbered-chapters'")
        levels = tuple(heading_levels)
        if not levels or any(level < 1 or level > 6 for level in levels):
            raise ValueError("Markdown heading levels must be between 1 and 6")
        self.profile = profile
        self.heading_levels = levels
        self.heading_pattern = heading_pattern
        self.document_id = document_id
        self.document_title = document_title

    @staticmethod
    def _atx_visible_text(text: str) -> str:
        """Mask HTML comments and fenced code while preserving source offsets."""

        def masked(value: str) -> str:
            return "".join("\n" if char == "\n" else " " for char in value)

        visible = re.sub(
            r"<!--.*?-->",
            lambda match: masked(match.group(0)),
            text,
            flags=re.DOTALL,
        )
        lines = visible.splitlines(keepends=True)
        fence: tuple[str, int] | None = None
        for index, line in enumerate(lines):
            without_ending = line.rstrip("\r\n")
            opening = re.match(r"^[ \t]{0,3}(?P<fence>`{3,}|~{3,})", without_ending)
            if fence is None and opening:
                marker = opening.group("fence")
                fence = (marker[0], len(marker))
                lines[index] = masked(line)
                continue
            if fence is not None:
                marker, length = fence
                closing = re.match(
                    rf"^[ \t]{{0,3}}{re.escape(marker)}{{{length},}}[ \t]*$",
                    without_ending,
                )
                lines[index] = masked(line)
                if closing:
                    fence = None
        return "".join(lines)

    def read_document(self, path: str | Path) -> SourceDocument:
        absolute, relative = self._resolve(path)
        try:
            text = absolute.read_text(encoding="utf-8")
        except OSError as error:
            raise ValueError(f"cannot read Markdown source: {absolute}") from error
        visible = self._atx_visible_text(text)
        title_match = re.search(
            r"^[ \t]{0,3}#(?!#)[ \t]+(.+?)[ \t]*#*[ \t]*$", visible, re.MULTILINE
        )
        title = self.document_title or (
            title_match.group(1).strip() if title_match else relative.stem.replace("-", " ").title()
        )
        self._texts[relative] = text
        pattern = self.heading_pattern
        if self.profile == "numbered-chapters" and pattern is None:
            pattern = self.NUMBERED_CHAPTER_PATTERN
        return SourceDocument(
            id=self.document_id or _slug(relative.with_suffix("").as_posix()),
            path=relative,
            format=self.format,
            title=title,
            metadata={
                "profile": self.profile,
                "heading_levels": self.heading_levels,
                **({"heading_pattern": pattern} if pattern is not None else {}),
            },
        )

    def discover_units(self, document: SourceDocument) -> tuple[WorkUnit, ...]:
        text = self._text(document)
        pattern_value = document.metadata.get("heading_pattern")
        if pattern_value is not None:
            pattern = re.compile(str(pattern_value), re.MULTILINE)
        else:
            marks = "|".join(re.escape("#" * level) for level in self.heading_levels)
            pattern = re.compile(
                rf"^[ \t]{{0,3}}(?P<marks>{marks})[ \t]+(?P<title>.+?)[ \t]*#*[ \t]*$",
                re.MULTILINE,
            )
        search_text = text if pattern_value is not None else self._atx_visible_text(text)
        matches = list(pattern.finditer(search_text))
        units: list[WorkUnit] = []
        occurrences: dict[int, int] = {}
        total_lines = _line_count(text)
        for index, match in enumerate(matches):
            title_value = match.groupdict().get("title") or match.group(0)
            title = title_value.strip()
            number_text = match.groupdict().get("number")
            ordinal = int(number_text) if number_text is not None else index + 1
            start = text.count("\n", 0, match.start()) + 1
            next_start = matches[index + 1].start() if index + 1 < len(matches) else len(text)
            end = (
                total_lines
                if next_start == len(text)
                else max(start, text.count("\n", 0, next_start))
            )
            legacy = document.metadata.get("profile") == "numbered-chapters"
            occurrences[ordinal] = occurrences.get(ordinal, 0) + 1
            suffix = f"-{occurrences[ordinal]:02d}" if occurrences[ordinal] > 1 else ""
            unit_id = (
                f"{document.id}/chapter-{ordinal:02d}{suffix}"
                if legacy
                else f"{document.id}/unit-{index + 1:02d}"
            )
            units.append(
                WorkUnit(
                    id=unit_id,
                    document=document,
                    title=title,
                    ordinal=ordinal,
                    source_span=SourceSpan(start, end),
                    metadata={"heading": match.group(0).strip()},
                )
            )
        return tuple(units)


class LatexAdapter(_FileAdapter):
    suffixes = frozenset({".tex"})
    format = "latex"
    LEVELS = ("part", "chapter", "section", "subsection")
    DEFAULT_VERBATIM_ENVIRONMENTS = frozenset(
        {"verbatim", "verbatim*", "Verbatim", "Verbatim*", "lstlisting", "minted", "comment"}
    )

    def __init__(
        self,
        *,
        root: str | Path | None = None,
        unit: str = "section",
        commands: Iterable[str] | None = None,
        verbatim_environments: Iterable[str] | None = None,
        follow_includes: bool = False,
        document_id: str | None = None,
        document_title: str | None = None,
    ) -> None:
        super().__init__(root=root)
        selected = tuple(commands) if commands is not None else (unit,)
        if not selected or any(command not in self.LEVELS for command in selected):
            raise ValueError("LaTeX boundaries must be part, chapter, section, or subsection")
        self.commands = selected
        self.verbatim_environments = frozenset(
            verbatim_environments or self.DEFAULT_VERBATIM_ENVIRONMENTS
        )
        self.follow_includes = follow_includes
        self.document_id = document_id
        self.document_title = document_title
        self._includes: dict[Path, tuple[SourceDocument, ...]] = {}

    @staticmethod
    def _strip_comment(line: str) -> str:
        for index, char in enumerate(line):
            if char != "%":
                continue
            backslashes = 0
            cursor = index - 1
            while cursor >= 0 and line[cursor] == "\\":
                backslashes += 1
                cursor -= 1
            if backslashes % 2 == 0:
                return line[:index]
        return line

    def _visible_lines(self, text: str) -> list[str]:
        visible: list[str] = []
        active: str | None = None
        begin = re.compile(r"\\begin\s*\{(?P<name>[^}]+)\}")
        for raw in text.splitlines():
            line = self._strip_comment(raw)
            if active is not None:
                visible.append("")
                if re.search(rf"\\end\s*\{{{re.escape(active)}\}}", line):
                    active = None
                continue
            match = begin.search(line)
            if match and match.group("name") in self.verbatim_environments:
                active = match.group("name")
                line = line[: match.start()]
            visible.append(line)
        if text.endswith("\n") and not visible:
            visible.append("")
        return visible

    def _read_one(self, path: Path, *, root_document: bool = False) -> SourceDocument:
        absolute, relative = self._resolve(path)
        try:
            text = absolute.read_text(encoding="utf-8")
        except OSError as error:
            raise ValueError(f"cannot read LaTeX source: {absolute}") from error
        self._texts[relative] = text
        visible = "\n".join(self._visible_lines(text))
        title_match = re.search(r"\\title\s*\{(?P<title>[^{}]*)\}", visible)
        title = (
            self.document_title
            if root_document and self.document_title is not None
            else title_match.group("title").strip()
            if title_match
            else relative.stem.replace("-", " ").title()
        )
        return SourceDocument(
            id=(
                self.document_id
                if root_document and self.document_id
                else _slug(relative.with_suffix("").as_posix())
            ),
            path=relative,
            format=self.format,
            title=title,
            metadata={
                "commands": self.commands,
                "follow_includes": self.follow_includes,
                "verbatim_environments": tuple(sorted(self.verbatim_environments)),
            },
        )

    def read_document(self, path: str | Path) -> SourceDocument:
        if self.root is None:
            self.root = Path(path).resolve().parent
        document = self._read_one(Path(path), root_document=True)
        if not self.follow_includes:
            self._includes[document.path] = ()
            return document

        loaded: dict[Path, SourceDocument] = {document.path: document}

        def visit(current: SourceDocument, stack: tuple[Path, ...]) -> None:
            if current.path in stack:
                cycle = (*stack[stack.index(current.path) :], current.path)
                raise ValueError(
                    "LaTeX include cycle: " + " -> ".join(item.as_posix() for item in cycle)
                )
            children: list[SourceDocument] = []
            visible = "\n".join(self._visible_lines(self._text(current)))
            for match in re.finditer(r"\\(?:input|include)\s*\{(?P<path>[^}]+)\}", visible):
                value = match.group("path").strip()
                include = Path(value)
                if include.suffix == "":
                    include = include.with_suffix(".tex")
                absolute = (
                    self.root / current.path.parent / include
                    if self.root
                    else Path.cwd() / current.path.parent / include
                ).resolve()
                _, relative = self._resolve(absolute)
                child = loaded.get(relative)
                if child is None:
                    child = self._read_one(absolute)
                    loaded[relative] = child
                children.append(child)
                visit(child, (*stack, current.path))
            self._includes[current.path] = tuple(children)

        visit(document, ())
        return document

    def _local_units(self, document: SourceDocument) -> tuple[WorkUnit, ...]:
        text = self._text(document)
        visible = "\n".join(self._visible_lines(text))
        commands = "|".join(re.escape(command) for command in self.commands)
        pattern = re.compile(
            rf"\\(?P<command>{commands})\*?\s*(?:\[[^\]]*\]\s*)?\{{(?P<title>[^{{}}]*)\}}"
        )
        matches = list(pattern.finditer(visible))
        total_lines = _line_count(text)
        units: list[WorkUnit] = []
        for index, match in enumerate(matches):
            start = visible.count("\n", 0, match.start()) + 1
            next_start = matches[index + 1].start() if index + 1 < len(matches) else len(visible)
            end = (
                total_lines
                if next_start == len(visible)
                else max(start, visible.count("\n", 0, next_start))
            )
            units.append(
                WorkUnit(
                    id=f"{document.id}/unit-{index + 1:02d}",
                    document=document,
                    title=match.group("title").strip(),
                    ordinal=index + 1,
                    source_span=SourceSpan(start, end),
                    metadata={"command": match.group("command")},
                )
            )
        return tuple(units)

    def discover_units(self, document: SourceDocument) -> tuple[WorkUnit, ...]:
        units: list[WorkUnit] = []
        visited: set[Path] = set()

        def collect(current: SourceDocument) -> None:
            if current.path in visited:
                return
            visited.add(current.path)
            units.extend(self._local_units(current))
            for child in self._includes.get(current.path, ()):
                collect(child)

        collect(document)
        return tuple(units)


class TextAdapter(_FileAdapter):
    suffixes = frozenset({".txt", ".text"})
    format = "text"

    def __init__(
        self,
        *,
        root: str | Path | None = None,
        heading_pattern: str | None = None,
        delimiter: str | None = None,
        document_id: str | None = None,
        document_title: str | None = None,
    ) -> None:
        super().__init__(root=root)
        if heading_pattern is not None and delimiter is not None:
            raise ValueError("plain text accepts either heading_pattern or delimiter, not both")
        self.heading_pattern = heading_pattern
        self.delimiter = delimiter
        self.document_id = document_id
        self.document_title = document_title

    def read_document(self, path: str | Path) -> SourceDocument:
        absolute, relative = self._resolve(path)
        try:
            text = absolute.read_text(encoding="utf-8")
        except OSError as error:
            raise ValueError(f"cannot read plain-text source: {absolute}") from error
        self._texts[relative] = text
        return SourceDocument(
            id=self.document_id or _slug(relative.with_suffix("").as_posix()),
            path=relative,
            format=self.format,
            title=self.document_title or relative.stem.replace("-", " ").replace("_", " ").title(),
            metadata={
                **({"heading_pattern": self.heading_pattern} if self.heading_pattern else {}),
                **({"delimiter": self.delimiter} if self.delimiter is not None else {}),
            },
        )

    def discover_units(self, document: SourceDocument) -> tuple[WorkUnit, ...]:
        text = self._text(document)
        total_lines = _line_count(text)
        heading = document.metadata.get("heading_pattern")
        delimiter = document.metadata.get("delimiter")
        if heading is None and delimiter is None:
            return (
                WorkUnit(
                    id=f"{document.id}/unit-01",
                    document=document,
                    title=document.title,
                    ordinal=1,
                    source_span=SourceSpan(1, total_lines),
                ),
            )
        if heading is not None:
            matches = list(re.compile(str(heading), re.MULTILINE).finditer(text))
            boundaries = [(match.start(), match.end(), match) for match in matches]
        else:
            assert delimiter is not None
            boundaries = [
                (match.start(), match.end(), match)
                for match in re.finditer(re.escape(str(delimiter)), text)
            ]
            # Delimiters separate chunks; the first chunk begins at line one.
            boundaries = [(0, 0, None), *boundaries]
        units: list[WorkUnit] = []
        for index, (start_offset, content_offset, match) in enumerate(boundaries):
            next_offset = boundaries[index + 1][0] if index + 1 < len(boundaries) else len(text)
            start = text.count("\n", 0, start_offset) + 1
            if match is not None and heading is None:
                start = text.count("\n", 0, content_offset) + 1
            end = (
                total_lines
                if next_offset == len(text)
                else max(start, text.count("\n", 0, next_offset))
            )
            groups = match.groupdict() if match is not None else {}
            title = groups.get("title") or (
                match.group(0).strip() if match is not None and heading else ""
            )
            title = title.strip() or f"{document.title} {index + 1}"
            units.append(
                WorkUnit(
                    id=f"{document.id}/unit-{index + 1:02d}",
                    document=document,
                    title=title,
                    ordinal=index + 1,
                    source_span=SourceSpan(start, end),
                )
            )
        return tuple(units)


def adapter_for_format(format: str, **options: Any) -> SourceAdapter:
    normalized = format.casefold()
    if normalized in {"markdown", "md"}:
        return MarkdownAdapter(**options)
    if normalized in {"latex", "tex"}:
        return LatexAdapter(**options)
    if normalized in {"text", "txt", "plain-text", "plaintext"}:
        return TextAdapter(**options)
    raise ValueError(f"unsupported source format: {format}")


def format_for_path(path: str | Path) -> str:
    suffix = Path(path).suffix.casefold()
    if suffix in MarkdownAdapter.suffixes:
        return "markdown"
    if suffix in LatexAdapter.suffixes:
        return "latex"
    if suffix in TextAdapter.suffixes:
        return "text"
    raise ValueError(f"unsupported source extension: {Path(path).suffix or '<none>'}")
