from __future__ import annotations

import json
import os
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from fnmatch import fnmatchcase
from pathlib import Path
from typing import Any

from paf.adapters import (
    LatexAdapter,
    MarkdownAdapter,
    SourceAdapter,
    TextAdapter,
    format_for_path,
)
from paf.models import SourceDocument, WorkUnit

DEFAULT_INCLUDES = ("**/*.md", "**/*.tex", "**/*.txt")
DEFAULT_IGNORED_DIRECTORIES = frozenset(
    {".git", ".paf", ".hg", ".svn", ".lake", "build", "dist", "out", "target", "_build"}
)


def glob_matches(path: str | Path, pattern: str) -> bool:
    """Match a normalized repository path, including root files for ``**/`` globs."""

    value = Path(path).as_posix()
    normalized = pattern.replace("\\", "/")
    if value.startswith("./"):
        value = value[2:]
    if normalized.startswith("./"):
        normalized = normalized[2:]
    variants = {normalized}
    pending = [normalized]
    while pending:
        current = pending.pop()
        if "/**/" in current:
            collapsed = current.replace("/**/", "/", 1)
            if collapsed not in variants:
                variants.add(collapsed)
                pending.append(collapsed)
    variants.update(item[3:] for item in tuple(variants) if item.startswith("**/"))
    return any(fnmatchcase(value, item) for item in variants)


def _selected(path: Path, includes: Sequence[str], excludes: Sequence[str]) -> bool:
    value = path.as_posix()
    selected = not includes
    for raw in includes:
        negated = raw.startswith("!")
        pattern = raw[1:] if negated else raw
        if glob_matches(value, pattern):
            selected = not negated
    for raw in excludes:
        negated = raw.startswith("!")
        pattern = raw[1:] if negated else raw
        if glob_matches(value, pattern):
            selected = negated
    return selected


def _format_name(value: str) -> str:
    normalized = value.casefold()
    return {
        "md": "markdown",
        "tex": "latex",
        "txt": "text",
        "plain-text": "text",
        "plaintext": "text",
    }.get(normalized, normalized)


@dataclass(frozen=True)
class ResolvedSources:
    documents: tuple[SourceDocument, ...]
    work_units: tuple[WorkUnit, ...]


class SourceResolver:
    """Resolve source roots into format-neutral documents and work units.

    Paths and identifiers are repository-relative, directory traversal never follows
    symlinked directories, and parsing only begins after the complete path set has
    been normalized and sorted.
    """

    def __init__(
        self,
        root: str | Path,
        *,
        include: Iterable[str] = DEFAULT_INCLUDES,
        exclude: Iterable[str] = (),
        rules: Iterable[Mapping[str, Any]] = (),
        dependencies: Mapping[str, Sequence[str]] | None = None,
        manifest: Sequence[str] | str | Path | None = None,
        ignore_defaults: bool = True,
        markdown_profile: str = "atx",
    ) -> None:
        self.root = Path(root).resolve()
        if not self.root.is_dir():
            raise ValueError(f"source repository root is not a directory: {self.root}")
        self.include = (include,) if isinstance(include, str) else tuple(include)
        self.exclude = (exclude,) if isinstance(exclude, str) else tuple(exclude)
        if not all(isinstance(item, str) and item.lstrip("!") for item in self.include):
            raise ValueError("source include patterns must be non-empty strings")
        if not all(isinstance(item, str) and item.lstrip("!") for item in self.exclude):
            raise ValueError("source exclude patterns must be non-empty strings")
        raw_rules = (rules,) if isinstance(rules, Mapping) else tuple(rules)
        self.rules = tuple(dict(rule) for rule in raw_rules)
        if not all(isinstance(rule.get("glob"), str) and rule["glob"] for rule in self.rules):
            raise ValueError("each source rule requires a non-empty glob")
        self.dependencies: dict[str, tuple[str, ...]] = {}
        for key, value in (dependencies or {}).items():
            if (
                not isinstance(key, str)
                or not key
                or isinstance(value, str)
                or not all(isinstance(item, str) and item for item in value)
            ):
                raise ValueError(
                    "source dependencies must map document paths or ids to lists of documents"
                )
            self.dependencies[key] = tuple(value)
        self.manifest = manifest
        self.ignore_defaults = ignore_defaults
        self.markdown_profile = markdown_profile
        self._adapters: dict[Path, SourceAdapter] = {}
        self._units: dict[Path, tuple[WorkUnit, ...]] = {}

    def _relative(self, path: Path, *, name: str = "source") -> Path:
        absolute = path.resolve()
        try:
            return Path(absolute.relative_to(self.root).as_posix())
        except ValueError as error:
            raise ValueError(f"{name} must be inside source repository root: {path}") from error

    def _ignored_directory(self, directory: Path) -> bool:
        if directory.is_symlink():
            return True
        name = directory.name
        return self.ignore_defaults and (
            name.startswith(".") or name.casefold() in DEFAULT_IGNORED_DIRECTORIES
        )

    def _ignored_path(self, relative: Path) -> bool:
        if not self.ignore_defaults:
            return False
        return any(
            part.startswith(".") or part.casefold() in DEFAULT_IGNORED_DIRECTORIES
            for part in relative.parts[:-1]
        )

    @staticmethod
    def _roots(roots: Iterable[str | Path] | str | Path) -> tuple[str | Path, ...]:
        if isinstance(roots, (str, Path)):
            return (roots,)
        return tuple(roots)

    def discover_paths(self, roots: Iterable[str | Path] | str | Path) -> tuple[Path, ...]:
        candidates: dict[Path, Path] = {}
        supplied = self._roots(roots)
        if not supplied:
            raise ValueError("source resolver requires at least one file or directory root")
        for raw_root in supplied:
            path = Path(raw_root)
            unresolved = path if path.is_absolute() else self.root / path
            explicitly_symlinked_directory = unresolved.is_symlink() and unresolved.is_dir()
            absolute = unresolved.resolve()
            relative_root = self._relative(absolute, name="source root")
            if not absolute.exists():
                raise ValueError(f"source root does not exist: {absolute}")
            if absolute.is_file():
                paths = (absolute,)
            elif absolute.is_dir():
                if explicitly_symlinked_directory or (
                    self._ignored_directory(absolute) and relative_root != Path(".")
                ):
                    paths = ()
                else:
                    discovered: list[Path] = []
                    for current, directories, files in os.walk(absolute, followlinks=False):
                        current_path = Path(current)
                        directories[:] = sorted(
                            (
                                name
                                for name in directories
                                if not self._ignored_directory(current_path / name)
                            ),
                            key=str.casefold,
                        )
                        discovered.extend(current_path / name for name in files)
                    paths = tuple(discovered)
            else:
                raise ValueError(f"source root must be a file or directory: {absolute}")
            for candidate in paths:
                relative = self._relative(candidate)
                if self._ignored_path(relative):
                    continue
                try:
                    format_for_path(relative)
                except ValueError:
                    continue
                if _selected(relative, self.include, self.exclude):
                    # De-duplicate overlapping roots and file aliases by canonical path.
                    candidates.setdefault(candidate.resolve(), relative)
        ordered = tuple(sorted(candidates.values(), key=lambda item: item.as_posix()))
        if not ordered:
            roots_text = ", ".join(str(item) for item in supplied)
            raise ValueError(f"no supported source documents matched roots: {roots_text}")
        return self._apply_manifest_to_paths(ordered)

    def _manifest_entries(self) -> tuple[str, ...]:
        if self.manifest is None:
            return ()
        if isinstance(self.manifest, (str, Path)):
            path = Path(self.manifest)
            absolute = path if path.is_absolute() else self.root / path
            try:
                text = absolute.read_text(encoding="utf-8")
            except OSError as error:
                raise ValueError(f"cannot read source manifest: {absolute}") from error
            if absolute.suffix.casefold() == ".json":
                value = json.loads(text)
                if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
                    raise ValueError("source manifest JSON must be a list of paths")
                return tuple(value)
            return tuple(
                line.strip()
                for line in text.splitlines()
                if line.strip() and not line.lstrip().startswith("#")
            )
        return tuple(self.manifest)

    def _apply_manifest_to_paths(self, paths: tuple[Path, ...]) -> tuple[Path, ...]:
        entries = tuple(
            value[2:] if value.startswith("./") else value
            for value in (Path(item).as_posix() for item in self._manifest_entries())
        )
        if not entries:
            return paths
        if len(entries) != len(set(entries)):
            raise ValueError("source manifest contains duplicate entries")
        by_path = {path.as_posix(): path for path in paths}
        ambiguous_stems: set[str] = set()
        for path in paths:
            stem = path.with_suffix("").as_posix()
            if stem in by_path and by_path[stem] != path:
                ambiguous_stems.add(stem)
            else:
                by_path[stem] = path
        for stem in ambiguous_stems:
            by_path.pop(stem, None)
        unknown = [entry for entry in entries if entry not in by_path]
        if unknown:
            raise ValueError(
                "source manifest references undiscovered documents: " + ", ".join(unknown)
            )
        ordered = tuple(by_path[entry] for entry in entries)
        if len(set(ordered)) != len(ordered):
            raise ValueError("source manifest contains duplicate entries")
        selected = {path.as_posix() for path in ordered}
        return ordered + tuple(path for path in paths if path.as_posix() not in selected)

    def _options(self, path: Path) -> dict[str, Any]:
        options: dict[str, Any] = {}
        for rule in self.rules:
            glob = rule.get("glob")
            if isinstance(glob, str) and glob_matches(path, glob):
                options.update({key: value for key, value in rule.items() if key != "glob"})
        return options

    def _adapter(self, path: Path) -> SourceAdapter:
        options = self._options(path)
        source_format = _format_name(str(options.get("format") or format_for_path(path)))
        if source_format == "markdown":
            level = {
                "part": 1,
                "chapter": 2,
                "section": 2,
                "subsection": 3,
                "h1": 1,
                "h2": 2,
                "h3": 3,
                "h4": 4,
                "h5": 5,
                "h6": 6,
            }.get(str(options.get("unit", "section")), 2)
            return MarkdownAdapter(
                root=self.root,
                profile=str(options.get("profile", self.markdown_profile)),
                heading_levels=(level,),
                heading_pattern=(
                    str(options["heading_pattern"]) if "heading_pattern" in options else None
                ),
            )
        if source_format == "latex":
            return LatexAdapter(
                root=self.root,
                unit=str(options.get("unit", "section")),
                follow_includes=bool(options.get("follow_includes", False)),
                verbatim_environments=tuple(options.get("verbatim_environments", ())) or None,
            )
        if source_format == "text":
            return TextAdapter(
                root=self.root,
                heading_pattern=(
                    str(options["heading_pattern"]) if "heading_pattern" in options else None
                ),
                delimiter=(str(options["delimiter"]) if "delimiter" in options else None),
            )
        raise ValueError(f"unsupported source format for {path}: {source_format}")

    @staticmethod
    def _reference_map(documents: tuple[SourceDocument, ...]) -> dict[str, str]:
        references: dict[str, str] = {}
        ambiguous: set[str] = set()
        for document in documents:
            keys = {document.id, document.path.as_posix(), document.path.with_suffix("").as_posix()}
            for key in keys:
                if key in references and references[key] != document.id:
                    ambiguous.add(key)
                else:
                    references[key] = document.id
        for key in ambiguous:
            references.pop(key, None)
        return references

    def resolve(self, roots: Iterable[str | Path] | str | Path) -> tuple[SourceDocument, ...]:
        documents: list[SourceDocument] = []
        for relative in self.discover_paths(roots):
            adapter = self._adapter(relative)
            document = adapter.read_document(self.root / relative)
            self._adapters[relative] = adapter
            self._units[relative] = adapter.discover_units(document)
            documents.append(document)
        result = tuple(documents)
        ids = [document.id for document in result]
        if len(ids) != len(set(ids)):
            raise ValueError("resolved source document ids must be unique")
        references = self._reference_map(result)
        configured: dict[str, tuple[str, ...]] = {}
        for raw_document, raw_dependencies in self.dependencies.items():
            document_id = references.get(
                Path(raw_document).as_posix(), references.get(raw_document)
            )
            if document_id is None:
                raise ValueError(f"source dependency references unknown document: {raw_document}")
            resolved_dependencies: list[str] = []
            for raw_dependency in raw_dependencies:
                dependency_id = references.get(
                    Path(raw_dependency).as_posix(), references.get(raw_dependency)
                )
                if dependency_id is None:
                    raise ValueError(
                        f"source dependency for {raw_document} references unknown document: "
                        f"{raw_dependency}"
                    )
                if dependency_id == document_id:
                    raise ValueError(f"source document {raw_document} cannot depend on itself")
                if dependency_id not in resolved_dependencies:
                    resolved_dependencies.append(dependency_id)
            configured[document_id] = tuple(resolved_dependencies)
        visiting: list[str] = []
        visited: set[str] = set()

        def visit(document_id: str) -> None:
            if document_id in visiting:
                cycle = (*visiting[visiting.index(document_id) :], document_id)
                raise ValueError("source dependency cycle: " + " -> ".join(cycle))
            if document_id in visited:
                return
            visiting.append(document_id)
            for dependency_id in configured.get(document_id, ()):
                visit(dependency_id)
            visiting.pop()
            visited.add(document_id)

        for document in result:
            visit(document.id)
        return tuple(
            replace(document, depends_on=configured.get(document.id, document.depends_on))
            for document in result
        )

    def discover_units(self, documents: Iterable[SourceDocument]) -> tuple[WorkUnit, ...]:
        units: list[WorkUnit] = []
        for document in documents:
            try:
                discovered = self._units[document.path]
            except KeyError as error:
                raise ValueError(
                    f"document was not read by this resolver: {document.path}"
                ) from error
            # Ensure dependency-enriched document objects are attached to the units.
            units.extend(replace(unit, document=document) for unit in discovered)
        return tuple(units)

    def resolve_all(self, roots: Iterable[str | Path] | str | Path) -> ResolvedSources:
        documents = self.resolve(roots)
        return ResolvedSources(documents, self.discover_units(documents))
