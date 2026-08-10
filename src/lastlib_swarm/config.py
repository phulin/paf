from __future__ import annotations

import re
import tomllib
from pathlib import Path
from typing import Any

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


def _table(data: dict[str, Any], key: str) -> dict[str, Any]:
    value = data.get(key, {})
    if not isinstance(value, dict):
        raise ValueError(f"{key} must be a TOML table")
    return value


def _render(value: str, variables: dict[str, str]) -> str:
    for key, replacement in variables.items():
        value = value.replace("{" + key + "}", replacement)
    return value


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
        books.append(
            BookConfig(
                id=raw["id"],
                title=raw["title"],
                source=Path(raw["source"]),
                lean_root=Path(raw["lean_root"]),
                module=raw["module"],
                depends_on=tuple(depends_on),
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
        model=str(swarm["model"]) if "model" in swarm else None,
        reasoning_effort=(str(swarm["reasoning_effort"]) if "reasoning_effort" in swarm else None),
        sandbox=str(swarm.get("sandbox", "workspace-write")),
        approve_for_me=bool(swarm.get("approve_for_me", True)),
        bypass_approvals_and_sandbox=bool(swarm.get("bypass_approvals_and_sandbox", False)),
        validation_timeout_seconds=float(swarm.get("validation_timeout_seconds", 1800)),
    )
    if settings.max_agents < 1:
        raise ValueError("swarm.max_agents must be positive")

    raw_stages = _table(data, "stages")
    stages: dict[Stage, StageConfig] = {}
    defaults = {
        Stage.FORMALIZE: 3,
        Stage.REVIEW: 5,
        Stage.PROVE: 10,
        Stage.REPAIR: 10,
    }
    for stage in Stage:
        raw = raw_stages.get(stage.value)
        if not isinstance(raw, dict) or not isinstance(raw.get("prompt"), str):
            raise ValueError(f"[stages.{stage.value}] must define prompt")
        max_rounds = int(raw.get("max_rounds", defaults[stage]))
        if max_rounds < 1:
            raise ValueError(f"stages.{stage.value}.max_rounds must be positive")
        prompt = _resolve(base, raw["prompt"])
        if not prompt.is_file():
            raise ValueError(f"prompt does not exist: {prompt}")
        stages[stage] = StageConfig(prompt=prompt, max_rounds=max_rounds)

    books = _read_books(data.get("books"))
    chapters = tuple(chapter for book in books for chapter in _discover_chapters(repo, book))
    return PipelineConfig(
        path=config_path,
        settings=settings,
        stages=stages,
        books=books,
        chapters=chapters,
    )
