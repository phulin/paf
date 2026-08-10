from __future__ import annotations

import re
import tomllib
from dataclasses import replace
from hashlib import sha256
from importlib.resources import files
from itertools import pairwise
from pathlib import Path
from typing import Any

from lastlib_swarm.corpus import build_corpus_schedule
from lastlib_swarm.models import (
    BookConfig,
    Chapter,
    PipelineConfig,
    Stage,
    StageConfig,
    SwarmSettings,
    as_string_dict,
)


def _resolve(base: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else (base / path).resolve()


def _repo_relative(repo: Path, value: str, *, name: str) -> Path:
    path = Path(value)
    resolved = path.resolve() if path.is_absolute() else (repo / path).resolve()
    try:
        return resolved.relative_to(repo)
    except ValueError as error:
        raise ValueError(f"{name} must be inside swarm.repo") from error


def _table(data: dict[str, Any], key: str) -> dict[str, Any]:
    value = data.get(key, {})
    if not isinstance(value, dict):
        raise ValueError(f"{key} must be a TOML table")
    return value


def _render(value: str, variables: dict[str, str]) -> str:
    for key, replacement in variables.items():
        value = value.replace("{" + key + "}", replacement)
    return value


STAGE_ROUNDS = {
    Stage.FORMALIZE: 3,
    Stage.REVIEW: 5,
    Stage.PROVE: 10,
    Stage.REPAIR: 10,
}


def standard_prompt_path(stage: Stage) -> Path:
    resource = files("lastlib_swarm.prompts").joinpath(f"{stage.value}.md")
    path = Path(str(resource))
    if not path.is_file():
        raise ValueError(f"packaged standard prompt is missing: {stage.value}")
    return path


def _stage_configs(raw_stages: dict[str, Any], base: Path) -> dict[Stage, StageConfig]:
    stages: dict[Stage, StageConfig] = {}
    for stage in Stage:
        raw = raw_stages.get(stage.value, {})
        if not isinstance(raw, dict):
            raise ValueError(f"[stages.{stage.value}] must be a table")
        prompt_value = raw.get("prompt")
        if prompt_value is None:
            prompt = standard_prompt_path(stage)
        elif isinstance(prompt_value, str):
            prompt = _resolve(base, prompt_value)
        else:
            raise ValueError(f"stages.{stage.value}.prompt must be a string")
        max_rounds = int(raw.get("max_rounds", STAGE_ROUNDS[stage]))
        if max_rounds < 1:
            raise ValueError(f"stages.{stage.value}.max_rounds must be positive")
        if not prompt.is_file():
            raise ValueError(f"prompt does not exist: {prompt}")
        stages[stage] = StageConfig(prompt=prompt, max_rounds=max_rounds)
    return stages


def _read_books(raw_books: Any) -> tuple[BookConfig, ...]:
    if not isinstance(raw_books, list) or not raw_books:
        raise ValueError("configuration must contain at least one [[books]] table")
    books: list[BookConfig] = []
    for raw in raw_books:
        if not isinstance(raw, dict):
            raise ValueError("each books item must be a table")
        for key in ("id", "title", "source", "lean_root", "module"):
            if not isinstance(raw.get(key), str) or not raw[key]:
                raise ValueError(f"books.{key} is required and must be a non-empty string")
        chapter_numbers = raw.get("chapters", [])
        if not isinstance(chapter_numbers, list) or not all(
            isinstance(number, int) and number > 0 for number in chapter_numbers
        ):
            raise ValueError("books.chapters must be a list of positive integers")
        scope = raw.get(
            "scope",
            ["{lean_root}/{chapter_path}.lean", "{lean_root}/{chapter_path}/**/*.lean"],
        )
        if not isinstance(scope, list) or not all(isinstance(item, str) for item in scope):
            raise ValueError("books.scope must be a list of strings")
        depends_on = raw.get("depends_on", [])
        if not isinstance(depends_on, list) or not all(
            isinstance(item, str) for item in depends_on
        ):
            raise ValueError("books.depends_on must be a list of book ids")
        efforts: dict[str, float | None] = {}
        for name in ("statement_effort", "proof_effort"):
            value = raw.get(name)
            if value is not None and (not isinstance(value, (int, float)) or value <= 0):
                raise ValueError(f"books.{name} must be a positive number")
            efforts[name] = float(value) if value is not None else None
        books.append(
            BookConfig(
                id=raw["id"],
                title=raw["title"],
                source=Path(raw["source"]),
                lean_root=Path(raw["lean_root"]),
                module=raw["module"],
                depends_on=tuple(depends_on),
                statement_effort=efforts["statement_effort"],
                proof_effort=efforts["proof_effort"],
                chapters=tuple(chapter_numbers),
                heading_pattern=str(raw.get("heading_pattern", BookConfig.heading_pattern)),
                chapter_path=str(raw.get("chapter_path", "Chapter{chapter_number_padded}")),
                chapter_module=str(
                    raw.get("chapter_module", "{module}.Chapter{chapter_number_padded}")
                ),
                build_command=str(
                    raw.get("build_command", "cd lean && lake build +{chapter_module}")
                ),
                scope=tuple(scope),
                context=as_string_dict(raw.get("context", {}), name="books.context"),
            )
        )
    ids = [book.id for book in books]
    if len(ids) != len(set(ids)):
        raise ValueError("book ids must be unique")
    known = set(ids)
    for book in books:
        missing = set(book.depends_on) - known
        if missing:
            names = ", ".join(sorted(missing))
            raise ValueError(f"book {book.id} depends on unconfigured books: {names}")
    return tuple(books)


def _discover_chapters(repo: Path, book: BookConfig) -> list[Chapter]:
    source_path = repo / book.source
    try:
        source_text = source_path.read_text(encoding="utf-8")
    except OSError as error:
        raise ValueError(f"cannot read source for {book.id}: {source_path}") from error
    pattern = re.compile(book.heading_pattern, re.MULTILINE)
    discovered: dict[int, str] = {}
    for match in pattern.finditer(source_text):
        number = int(match.group("number"))
        discovered[number] = match.group("title").strip()
    selected = book.chapters or tuple(sorted(discovered))
    missing = [number for number in selected if number not in discovered]
    if missing:
        raise ValueError(f"book {book.id} is missing source headings for chapters {missing}")
    chapters: list[Chapter] = []
    for number in selected:
        variables = {
            "book_id": book.id,
            "book_title": book.title,
            "chapter_number": str(number),
            "chapter_number_padded": f"{number:02d}",
            "chapter_title": discovered[number],
            "source": book.source.as_posix(),
            "lean_root": book.lean_root.as_posix(),
            "module": book.module,
            **book.context,
        }
        chapter_path = _render(book.chapter_path, variables)
        variables["chapter_path"] = chapter_path
        chapter_module = _render(book.chapter_module, variables)
        variables["chapter_module"] = chapter_module
        build_command = _render(book.build_command, variables)
        variables["build_command"] = build_command
        chapters.append(
            Chapter(
                book_id=book.id,
                book_title=book.title,
                number=number,
                title=discovered[number],
                source=book.source,
                lean_root=book.lean_root,
                module=book.module,
                chapter_path=chapter_path,
                chapter_module=chapter_module,
                build_command=build_command,
                scope=tuple(_render(item, variables) for item in book.scope),
                depends_on_books=book.depends_on,
                context=book.context,
            )
        )
    return chapters


def load_config(path: str | Path) -> PipelineConfig:
    config_path = Path(path).resolve()
    with config_path.open("rb") as handle:
        data = tomllib.load(handle)
    base = config_path.parent
    swarm = _table(data, "swarm")
    repo = _resolve(base, str(swarm.get("repo", ".")))
    state_dir = _resolve(repo, str(swarm.get("state_dir", ".swarm")))
    settings = SwarmSettings(
        repo=repo,
        state_dir=state_dir,
        max_agents=int(swarm.get("max_agents", 16)),
        codex_bin=str(swarm.get("codex_bin", "codex")),
        model=str(swarm.get("model", "gpt-5.6-luna")),
        reasoning_effort=str(swarm.get("reasoning_effort", "max")),
        sandbox=str(swarm.get("sandbox", "workspace-write")),
        approve_for_me=bool(swarm.get("approve_for_me", True)),
        bypass_approvals_and_sandbox=bool(swarm.get("bypass_approvals_and_sandbox", False)),
        agent_timeout_seconds=float(swarm.get("agent_timeout_seconds", 7200)),
        validation_timeout_seconds=float(swarm.get("validation_timeout_seconds", 1800)),
        isolation=str(swarm.get("isolation", "auto")),
        lean_mcp=bool(swarm.get("lean_mcp", True)),
        lean_project=_repo_relative(
            repo,
            str(swarm.get("lean_project", "lean")),
            name="swarm.lean_project",
        ),
        lean_mcp_tool_timeout_seconds=float(swarm.get("lean_mcp_tool_timeout_seconds", 300)),
    )
    if settings.max_agents < 1:
        raise ValueError("swarm.max_agents must be positive")
    if settings.isolation not in {"auto", "fuse-overlay", "shared"}:
        raise ValueError("swarm.isolation must be auto, fuse-overlay, or shared")
    if settings.lean_mcp_tool_timeout_seconds <= 0:
        raise ValueError("swarm.lean_mcp_tool_timeout_seconds must be positive")

    stages = _stage_configs(_table(data, "stages"), base)

    books = _read_books(data.get("books"))
    chapters = tuple(chapter for book in books for chapter in _discover_chapters(repo, book))
    # Validate the complete graph while loading, before any agents are launched.
    build_corpus_schedule(books, chapters, phase="statements")
    return PipelineConfig(
        path=config_path,
        settings=settings,
        stages=stages,
        books=books,
        chapters=chapters,
    )


def _repository_root(target: Path) -> Path:
    for candidate in (target.parent, *target.parents):
        if (candidate / ".git").exists():
            return candidate
    current = Path.cwd().resolve()
    for candidate in (current, *current.parents):
        if (candidate / ".git").exists():
            return candidate
    return current


def _pascal_case(value: str) -> str:
    words = re.findall(r"[A-Za-z0-9]+", value)
    return "".join(word[:1].upper() + word[1:] for word in words) or "Formalization"


def _source_title(target: Path) -> str:
    text = target.read_text(encoding="utf-8")
    match = re.search(r"^#\s+(.+?)\s*$", text, re.MULTILINE)
    if match:
        return match.group(1).strip()
    name = re.sub(r"^\d+[-_]", "", target.stem)
    return name.replace("-", " ").replace("_", " ").title()


def _existing_lean_stem(repo: Path, number: int) -> str | None:
    root = repo / "lean" / "LastLib"
    stems = {path.stem if path.is_file() else path.name for path in root.glob(f"Book{number:02d}*")}
    return next(iter(stems)) if len(stems) == 1 else None


def _infer_book(repo: Path, source_path: Path) -> BookConfig:
    try:
        source = source_path.relative_to(repo)
    except ValueError:
        source = source_path
    title = _source_title(source_path)
    prefix = re.match(r"^(?P<number>\d+)[-_]", source_path.name)
    if prefix:
        number = int(prefix.group("number"))
        book_id = f"book{number:02d}"
        lean_stem = _existing_lean_stem(repo, number) or f"Book{number:02d}{_pascal_case(title)}"
    else:
        book_id = re.sub(r"[^a-z0-9]+", "-", source_path.stem.lower()).strip("-")
        lean_stem = f"Book{_pascal_case(title)}"
    return BookConfig(
        id=book_id,
        title=title,
        source=source,
        lean_root=Path("lean") / "LastLib" / lean_stem,
        module=f"LastLib.{lean_stem}",
    )


def infer_config(target: str | Path) -> PipelineConfig:
    source_path = Path(target).resolve()
    if source_path.is_dir():
        return infer_corpus((source_path,))
    if not source_path.is_file() or source_path.suffix.lower() != ".md":
        raise ValueError(f"target must be an existing Markdown file or directory: {source_path}")
    repo = _repository_root(source_path)
    book = _infer_book(repo, source_path)
    settings = SwarmSettings(
        repo=repo,
        state_dir=repo / ".swarm" / book.id,
        model="gpt-5.6-luna",
        reasoning_effort="max",
    )
    chapters = tuple(_discover_chapters(repo, book))
    return PipelineConfig(
        path=source_path,
        settings=settings,
        stages=_stage_configs({}, repo),
        books=(book,),
        chapters=chapters,
    )


def _expand_markdown_targets(targets: tuple[str | Path, ...]) -> tuple[Path, ...]:
    expanded: list[Path] = []
    for target in targets:
        path = Path(target).resolve()
        if path.is_dir():
            matches = sorted(item for item in path.glob("*.md") if item.is_file())
            if not matches:
                raise ValueError(f"directory contains no Markdown books: {path}")
            expanded.extend(matches)
        elif path.is_file() and path.suffix.lower() == ".md":
            expanded.append(path)
        else:
            raise ValueError(f"target must be an existing Markdown file or directory: {path}")
    unique = tuple(dict.fromkeys(expanded))
    if not unique:
        raise ValueError("corpus requires at least one Markdown target")
    return unique


def parse_book_dependencies(path: str | Path) -> dict[str, tuple[str, ...]]:
    """Read Mermaid `B01 --> B02 --> ...` edges from a dependency document."""

    dependency_path = Path(path).resolve()
    try:
        text = dependency_path.read_text(encoding="utf-8")
    except OSError as error:
        raise ValueError(f"cannot read book dependency graph: {dependency_path}") from error
    dependencies: dict[str, set[str]] = {}
    for line in text.splitlines():
        if "-->" not in line:
            continue
        nodes = re.findall(r"\bB(?P<number>\d+)\b", line, re.IGNORECASE)
        for prerequisite, dependent in pairwise(nodes):
            dependent_id = f"book{int(dependent):02d}"
            dependencies.setdefault(dependent_id, set()).add(f"book{int(prerequisite):02d}")
    return {key: tuple(sorted(value)) for key, value in dependencies.items()}


def infer_corpus(
    targets: tuple[str | Path, ...],
    *,
    dependency_file: str | Path | None = None,
) -> PipelineConfig:
    source_paths = _expand_markdown_targets(targets)
    repo = _repository_root(source_paths[0])
    for source_path in source_paths[1:]:
        if _repository_root(source_path) != repo:
            raise ValueError("all corpus targets must belong to the same repository")

    books = tuple(_infer_book(repo, source_path) for source_path in source_paths)
    ids = [book.id for book in books]
    if len(ids) != len(set(ids)):
        raise ValueError("inferred book ids must be unique; use a TOML config to disambiguate")

    graph_path = Path(dependency_file).resolve() if dependency_file is not None else None
    if graph_path is None and (repo / "BOOK_DEPENDENCIES.md").is_file():
        graph_path = repo / "BOOK_DEPENDENCIES.md"
    graph = parse_book_dependencies(graph_path) if graph_path is not None else {}
    selected = set(ids)
    books = tuple(
        replace(book, depends_on=tuple(item for item in graph.get(book.id, ()) if item in selected))
        for book in books
    )
    chapters = tuple(chapter for book in books for chapter in _discover_chapters(repo, book))
    build_corpus_schedule(books, chapters, phase="statements")
    identity = "\n".join(sorted(source_path.as_posix() for source_path in source_paths))
    corpus_id = sha256(identity.encode()).hexdigest()[:10]
    settings = SwarmSettings(
        repo=repo,
        state_dir=repo / ".swarm" / f"corpus-{corpus_id}",
        model="gpt-5.6-luna",
        reasoning_effort="max",
    )
    return PipelineConfig(
        path=graph_path or repo,
        settings=settings,
        stages=_stage_configs({}, repo),
        books=books,
        chapters=chapters,
    )


def resolve_config(
    *,
    config: str | Path | None,
    target: str | Path | None,
    dependency_file: str | Path | None = None,
) -> PipelineConfig:
    if config is not None and target is not None:
        raise ValueError("pass either --config or a Markdown target, not both")
    if config is not None:
        if dependency_file is not None:
            raise ValueError("--dependencies is only used with inferred Markdown targets")
        return load_config(config)
    if target is not None:
        path = Path(target)
        if path.is_dir():
            return infer_corpus((path,), dependency_file=dependency_file)
        if dependency_file is not None:
            return infer_corpus((path,), dependency_file=dependency_file)
        return infer_config(path)
    default = Path("swarm.toml")
    if default.is_file():
        return load_config(default)
    raise ValueError("pass a target .md or --config; no swarm.toml was found")
