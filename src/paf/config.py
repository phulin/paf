from __future__ import annotations

import re
import shlex
import tomllib
from dataclasses import replace
from importlib.resources import files
from itertools import pairwise
from pathlib import Path
from typing import Any

from paf.adapters import LatexAdapter, MarkdownAdapter, TextAdapter, format_for_path
from paf.backends import LeanBackend, TargetTemplates, lean_backend_from_config
from paf.corpus import build_corpus_schedule
from paf.hashing import digest_text, stable_digest_text
from paf.models import (
    BookConfig,
    Chapter,
    PipelineConfig,
    ShepherdSettings,
    Stage,
    StageConfig,
    SwarmSettings,
    WorkUnit,
    as_string_dict,
)
from paf.project import Project, ProjectResolver
from paf.resolver import DEFAULT_INCLUDES, SourceResolver, glob_matches


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
    Stage.DISCOVER: 1,
    Stage.FORMALIZE: 3,
    Stage.REVIEW: 3,
    Stage.PROVE: 3,
}

STAGE_MODELS = {
    Stage.DISCOVER: "gpt-5.6-luna",
}

STAGE_REASONING_EFFORTS = {
    Stage.DISCOVER: "xhigh",
}

STAGE_MAX_AGENTS = {
    Stage.DISCOVER: 40,
}

STAGE_CHUNK_SIZES = {
    Stage.PROVE: 4,
}


def standard_prompt_path(stage: Stage) -> Path:
    resource = files("paf.prompts").joinpath(f"{stage.value}.md")
    path = Path(str(resource))
    if not path.is_file():
        raise ValueError(f"packaged standard prompt is missing: {stage.value}")
    return path


def _stage_configs(raw_stages: dict[str, Any], base: Path) -> dict[Stage, StageConfig]:
    stages: dict[Stage, StageConfig] = {}
    legacy_workflow = "discover" not in raw_stages and any(
        name in raw_stages for name in ("fixup", "repair")
    )
    for stage in Stage:
        if legacy_workflow and stage is Stage.DISCOVER:
            raw = raw_stages.get("formalize", {})
        elif legacy_workflow and stage is Stage.FORMALIZE:
            raw = raw_stages.get("fixup", raw_stages.get("repair", {}))
        else:
            raw = raw_stages.get(stage.value, {})
        if stage is Stage.FORMALIZE and not raw:
            # Accept the pre-discovery name while reading an older config.
            raw = raw_stages.get("fixup", raw_stages.get("repair", {}))
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
        max_agents_value = raw.get("max_agents", STAGE_MAX_AGENTS.get(stage))
        if max_agents_value is not None and stage is not Stage.DISCOVER:
            raise ValueError("max_agents is only supported for stages.discover")
        max_agents = int(max_agents_value) if max_agents_value is not None else None
        if max_agents is not None and max_agents < 1:
            raise ValueError(f"stages.{stage.value}.max_agents must be positive")
        chunk_size_value = raw.get("chunk_size", STAGE_CHUNK_SIZES.get(stage))
        if chunk_size_value is not None and stage is not Stage.PROVE:
            raise ValueError("chunk_size is only supported for stages.prove")
        chunk_size = int(chunk_size_value) if chunk_size_value is not None else None
        if chunk_size is not None and chunk_size < 1:
            raise ValueError(f"stages.{stage.value}.chunk_size must be positive")
        if not prompt.is_file():
            raise ValueError(f"prompt does not exist: {prompt}")
        model_value = raw.get("model", STAGE_MODELS.get(stage))
        if model_value is not None and not isinstance(model_value, str):
            raise ValueError(f"stages.{stage.value}.model must be a string")
        reasoning_value = raw.get("reasoning_effort", STAGE_REASONING_EFFORTS.get(stage))
        if reasoning_value is not None and not isinstance(reasoning_value, str):
            raise ValueError(f"stages.{stage.value}.reasoning_effort must be a string")
        unchanged_retry_limit = int(raw.get("unchanged_retry_limit", 2))
        if stage is not Stage.PROVE and "unchanged_retry_limit" in raw:
            raise ValueError("retry settings are only supported for stages.prove")
        if unchanged_retry_limit < 1:
            raise ValueError("stages.prove.unchanged_retry_limit must be positive")
        stages[stage] = StageConfig(
            prompt=prompt,
            max_rounds=max_rounds,
            max_agents=max_agents,
            chunk_size=chunk_size,
            model=model_value,
            reasoning_effort=reasoning_value,
            unchanged_retry_limit=unchanged_retry_limit,
        )
    return stages


_SOURCE_RULE_KEYS = {
    "glob",
    "format",
    "profile",
    "unit",
    "follow_includes",
    "heading_pattern",
    "delimiter",
    "verbatim_environments",
}

_SOURCE_DISCOVERY_KEYS = {
    "roots",
    "include",
    "exclude",
    "dependencies",
    "manifest",
    "ignore_defaults",
}


def _read_source_settings(
    data: dict[str, Any],
) -> tuple[dict[str, Any], tuple[dict[str, Any], ...], dict[str, Any]]:
    raw = data.get("sources", {})
    if not isinstance(raw, dict):
        raise ValueError("sources must be a TOML table")
    rules_raw = raw.get("rules", [])
    if not isinstance(rules_raw, list):
        raise ValueError("sources.rules must be an array of tables")
    defaults = {key: value for key, value in raw.items() if key in _SOURCE_RULE_KEYS - {"glob"}}
    unknown_source_keys = set(raw) - _SOURCE_RULE_KEYS - _SOURCE_DISCOVERY_KEYS - {"rules"}
    if unknown_source_keys:
        raise ValueError(f"unknown sources keys: {', '.join(sorted(unknown_source_keys))}")
    discovery = {key: raw[key] for key in _SOURCE_DISCOVERY_KEYS if key in raw}
    rules: list[dict[str, Any]] = []
    for rule in rules_raw:
        if not isinstance(rule, dict):
            raise ValueError("each sources.rules item must be a table")
        unknown = set(rule) - _SOURCE_RULE_KEYS
        if unknown:
            raise ValueError(f"unknown sources.rules keys: {', '.join(sorted(unknown))}")
        if not isinstance(rule.get("glob"), str) or not rule["glob"]:
            raise ValueError("sources.rules.glob is required and must be a non-empty string")
        rules.append(dict(rule))
    for options, name in ((defaults, "sources"), *((rule, "sources.rules") for rule in rules)):
        format_value = options.get("format")
        if format_value is not None and str(format_value).casefold() not in {
            "markdown",
            "md",
            "latex",
            "tex",
            "text",
            "txt",
            "plain-text",
            "plaintext",
        }:
            raise ValueError(f"{name}.format is unsupported: {format_value}")
        if "follow_includes" in options and not isinstance(options["follow_includes"], bool):
            raise ValueError(f"{name}.follow_includes must be a boolean")
        for key in ("profile", "unit", "heading_pattern", "delimiter"):
            if key in options and not isinstance(options[key], str):
                raise ValueError(f"{name}.{key} must be a string")
        heading_pattern = options.get("heading_pattern")
        if isinstance(heading_pattern, str):
            try:
                re.compile(heading_pattern)
            except re.error as error:
                raise ValueError(f"{name}.heading_pattern is not a valid regex") from error
        if options.get("delimiter") == "":
            raise ValueError(f"{name}.delimiter must not be empty")
        environments = options.get("verbatim_environments")
        if environments is not None and (
            not isinstance(environments, list)
            or not all(isinstance(item, str) for item in environments)
        ):
            raise ValueError(f"{name}.verbatim_environments must be a list of strings")
    for key in ("roots", "include", "exclude"):
        value = discovery.get(key)
        if value is not None and (
            not isinstance(value, list) or not all(isinstance(item, str) and item for item in value)
        ):
            raise ValueError(f"sources.{key} must be a list of non-empty strings")
    dependencies = discovery.get("dependencies")
    if dependencies is not None and (
        not isinstance(dependencies, dict)
        or not all(
            isinstance(key, str)
            and key
            and isinstance(value, list)
            and all(isinstance(item, str) and item for item in value)
            for key, value in dependencies.items()
        )
    ):
        raise ValueError(
            "sources.dependencies must map document paths or ids to lists of documents"
        )
    manifest = discovery.get("manifest")
    if manifest is not None and not (
        isinstance(manifest, str)
        or (isinstance(manifest, list) and all(isinstance(item, str) and item for item in manifest))
        or isinstance(manifest, dict)
    ):
        raise ValueError(
            "sources.manifest must be a path, a list of document paths, or an extraction table"
        )
    if "ignore_defaults" in discovery and not isinstance(discovery["ignore_defaults"], bool):
        raise ValueError("sources.ignore_defaults must be a boolean")
    return defaults, tuple(rules), discovery


def _source_options(
    source: str, raw: dict[str, Any], defaults: dict[str, Any], rules: tuple[dict[str, Any], ...]
) -> dict[str, Any]:
    options = dict(defaults)
    options.update({key: raw[key] for key in _SOURCE_RULE_KEYS - {"glob"} if key in raw})
    normalized = Path(source).as_posix()
    for rule in rules:
        if glob_matches(normalized, str(rule["glob"])):
            options.update({key: value for key, value in rule.items() if key != "glob"})
    return options


def _read_books(
    raw_books: Any,
    *,
    source_defaults: dict[str, Any] | None = None,
    source_rules: tuple[dict[str, Any], ...] = (),
) -> tuple[BookConfig, ...]:
    if not isinstance(raw_books, list) or not raw_books:
        raise ValueError("configuration must contain at least one [[books]] table")
    books: list[BookConfig] = []
    for raw in raw_books:
        if not isinstance(raw, dict):
            raise ValueError("each books item must be a table")
        for key in ("id", "title", "source", "lean_root", "module"):
            if not isinstance(raw.get(key), str) or not raw[key]:
                raise ValueError(f"books.{key} is required and must be a non-empty string")
        options = _source_options(raw["source"], raw, source_defaults or {}, source_rules)
        source_format = str(options.get("format") or format_for_path(raw["source"])).casefold()
        source_format = {
            "md": "markdown",
            "tex": "latex",
            "txt": "text",
            "plain-text": "text",
            "plaintext": "text",
        }.get(source_format, source_format)
        default_profile = "numbered-chapters" if source_format == "markdown" else "default"
        adapter_profile = str(options.get("profile", default_profile))
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
                format=source_format,
                adapter_profile=adapter_profile,
                unit=str(options["unit"]) if "unit" in options else None,
                follow_includes=bool(options.get("follow_includes", False)),
                delimiter=str(options["delimiter"]) if "delimiter" in options else None,
                verbatim_environments=tuple(options.get("verbatim_environments", ())),
                heading_pattern=(
                    str(options["heading_pattern"])
                    if "heading_pattern" in options
                    else BookConfig.heading_pattern
                    if source_format == "markdown" and adapter_profile == "numbered-chapters"
                    else None
                ),
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
    if book.format == "markdown":
        markdown_level = {
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
        }.get(book.unit or "section", 2)
        adapter = MarkdownAdapter(
            root=repo,
            document_id=book.id,
            document_title=book.title,
            profile=book.adapter_profile,
            heading_levels=(markdown_level,),
            heading_pattern=book.heading_pattern,
        )
    elif book.format == "latex":
        adapter = LatexAdapter(
            root=repo,
            document_id=book.id,
            document_title=book.title,
            unit=book.unit or "section",
            follow_includes=book.follow_includes,
            verbatim_environments=book.verbatim_environments or None,
        )
    elif book.format == "text":
        adapter = TextAdapter(
            root=repo,
            document_id=book.id,
            document_title=book.title,
            heading_pattern=book.heading_pattern,
            delimiter=book.delimiter,
        )
    else:
        raise ValueError(f"unsupported source format for {book.id}: {book.format}")
    document = adapter.read_document(repo / book.source)
    discovered_units = adapter.discover_units(document)
    discovered = {unit.ordinal: unit for unit in discovered_units}
    if len(discovered) != len(discovered_units):
        raise ValueError(f"source {book.id} has duplicate unit ordinals")
    selected = book.chapters or tuple(unit.ordinal for unit in discovered_units)
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
            "chapter_title": discovered[number].title,
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
                title=discovered[number].title,
                source=discovered[number].source,
                lean_root=book.lean_root,
                module=book.module,
                chapter_path=chapter_path,
                chapter_module=chapter_module,
                build_command=build_command,
                scope=tuple(_render(item, variables) for item in book.scope),
                depends_on_books=book.depends_on,
                context=book.context,
                source_span=discovered[number].source_span,
            )
        )
    return chapters


def _chapter_from_work_unit(unit: WorkUnit) -> Chapter:
    """Expose a canonical target-mapped unit to legacy extension call sites."""

    target = unit._target()
    return Chapter(
        book_id=unit.document_id,
        book_title=unit.document.title,
        number=unit.ordinal,
        title=unit.title,
        source=unit.source,
        lean_root=target.root,
        module=target.module,
        chapter_path=target.path,
        chapter_module=target.unit_module,
        build_command=target.build_command,
        scope=target.scope,
        depends_on_books=unit.document.depends_on,
        context=unit.context,
        source_span=unit.source_span,
    )


def load_config(path: str | Path, *, project: Project | None = None) -> PipelineConfig:
    config_path = Path(path).resolve()
    project = project or ProjectResolver().resolve(config=config_path)
    with config_path.open("rb") as handle:
        data = tomllib.load(handle)
    base = config_path.parent
    swarm = _table(data, "swarm")
    if "lean_mcp" in swarm:
        raise ValueError("swarm.lean_mcp was removed; Lean MCP is always enabled")
    if "interface_invalidation" in swarm:
        raise ValueError(
            "swarm.interface_invalidation was removed; interface fingerprints now use "
            "one baseline-first invalidation policy"
        )
    repo = project.repository_path(str(swarm.get("repo", ".")), base=base)
    project = project.bind(root=repo, config_path=config_path)
    state_dir = project.state_path(str(swarm.get("state_dir", ".paf")))
    settings = SwarmSettings(
        repo=repo,
        state_dir=state_dir,
        max_agents=int(swarm.get("max_agents", 16)),
        codex_bin=str(swarm.get("codex_bin", "codex")),
        model=str(swarm.get("model", "gpt-5.6-luna")),
        reasoning_effort=str(swarm.get("reasoning_effort", "xhigh")),
        sandbox=str(swarm.get("sandbox", "danger-full-access")),
        approve_for_me=bool(swarm.get("approve_for_me", False)),
        bypass_approvals_and_sandbox=bool(swarm.get("bypass_approvals_and_sandbox", True)),
        agent_timeout_seconds=float(swarm.get("agent_timeout_seconds", 7200)),
        capacity_resume_attempts=int(swarm.get("capacity_resume_attempts", 10)),
        capacity_resume_delay_seconds=float(swarm.get("capacity_resume_delay_seconds", 15)),
        capacity_resume_max_delay_seconds=float(
            swarm.get("capacity_resume_max_delay_seconds", 120)
        ),
        codex_fd_recycle_threshold=int(swarm.get("codex_fd_recycle_threshold", 256)),
        codex_fd_recycle_attempts=int(swarm.get("codex_fd_recycle_attempts", 20)),
        validation_timeout_seconds=float(swarm.get("validation_timeout_seconds", 1800)),
        isolation=str(swarm.get("isolation", "auto")),
        cache_compaction_layers=int(swarm.get("cache_compaction_layers", 32)),
        lean_project=_repo_relative(
            repo,
            str(swarm.get("lean_project", "lean")),
            name="swarm.lean_project",
        ),
        lean_mcp_tool_timeout_seconds=float(swarm.get("lean_mcp_tool_timeout_seconds", 300)),
    )
    if settings.max_agents < 1:
        raise ValueError("swarm.max_agents must be positive")
    if settings.capacity_resume_attempts < 0:
        raise ValueError("swarm.capacity_resume_attempts must be nonnegative")
    if settings.capacity_resume_delay_seconds < 0:
        raise ValueError("swarm.capacity_resume_delay_seconds must be nonnegative")
    if settings.capacity_resume_max_delay_seconds < 0:
        raise ValueError("swarm.capacity_resume_max_delay_seconds must be nonnegative")
    if settings.codex_fd_recycle_threshold < 0:
        raise ValueError("swarm.codex_fd_recycle_threshold must be nonnegative")
    if settings.codex_fd_recycle_attempts < 0:
        raise ValueError("swarm.codex_fd_recycle_attempts must be nonnegative")
    if settings.isolation not in {"auto", "fuse-overlay", "shared"}:
        raise ValueError("swarm.isolation must be auto, fuse-overlay, or shared")
    if settings.cache_compaction_layers < 2:
        raise ValueError("swarm.cache_compaction_layers must be at least 2")
    if settings.lean_mcp_tool_timeout_seconds <= 0:
        raise ValueError("swarm.lean_mcp_tool_timeout_seconds must be positive")

    stages = _stage_configs(_table(data, "stages"), base)
    raw_shepherd = _table(data, "shepherd")
    shepherd = ShepherdSettings(
        enabled=bool(raw_shepherd.get("enabled", True)),
        model=str(raw_shepherd.get("model", "gpt-5.6-sol")),
        reasoning_effort=str(raw_shepherd.get("reasoning_effort", "medium")),
        worker_model=str(raw_shepherd.get("worker_model", "gpt-5.6-luna")),
        worker_reasoning_effort=str(raw_shepherd.get("worker_reasoning_effort", "xhigh")),
        interval_seconds=float(raw_shepherd.get("interval_seconds", 1200)),
        failure_threshold=int(raw_shepherd.get("failure_threshold", 10)),
        maximum_failures_per_sweep=int(raw_shepherd.get("maximum_failures_per_sweep", 50)),
        maximum_work_units_per_sweep=int(raw_shepherd.get("maximum_work_units_per_sweep", 32)),
        maximum_consecutive_no_progress_sweeps=int(
            raw_shepherd.get(
                "maximum_consecutive_no_progress_sweeps",
                raw_shepherd.get("maximum_sweeps_per_invocation", 3),
            )
        ),
    )
    if shepherd.interval_seconds <= 0:
        raise ValueError("shepherd.interval_seconds must be positive")
    if shepherd.failure_threshold < 1:
        raise ValueError("shepherd.failure_threshold must be positive")
    if shepherd.maximum_failures_per_sweep < 1:
        raise ValueError("shepherd.maximum_failures_per_sweep must be positive")
    if shepherd.maximum_work_units_per_sweep < 1:
        raise ValueError("shepherd.maximum_work_units_per_sweep must be positive")
    if shepherd.maximum_consecutive_no_progress_sweeps < 1:
        raise ValueError("shepherd.maximum_consecutive_no_progress_sweeps must be positive")

    source_defaults, source_rules, source_discovery = _read_source_settings(data)
    if "backend" in data and "target" in data:
        raise ValueError("configure [backend] or the [target] alias, not both")
    raw_backend = data.get("backend", data.get("target", {}))
    if not isinstance(raw_backend, dict):
        raise ValueError("backend must be a TOML table")
    backend = lean_backend_from_config(
        raw_backend,
        repo=repo,
        legacy_project=settings.lean_project,
        legacy_timeout=settings.lean_mcp_tool_timeout_seconds,
    )
    settings = replace(
        settings,
        lean_project=backend.project,
        lean_mcp_tool_timeout_seconds=backend.mcp_tool_timeout_seconds,
    )
    raw_books = data.get("books")
    source_roots = tuple(Path(item) for item in source_discovery.get("roots", ()))
    source_include = tuple(source_discovery.get("include", DEFAULT_INCLUDES))
    source_exclude = tuple(source_discovery.get("exclude", ()))
    if raw_books is None:
        if not source_roots:
            raise ValueError("configuration must contain [[books]] or non-empty sources.roots")
        resolver = SourceResolver(
            repo,
            include=source_include,
            exclude=source_exclude,
            rules=({"glob": "**/*", **source_defaults}, *source_rules),
            dependencies=source_discovery.get("dependencies"),
            manifest=source_discovery.get("manifest"),
            ignore_defaults=bool(source_discovery.get("ignore_defaults", True)),
        )
        resolved_sources = resolver.resolve_all(source_roots)
        documents = resolved_sources.documents
        books = tuple(
            _inferred_book_from_document(
                repo,
                document,
                _source_options(document.path.as_posix(), {}, source_defaults, source_rules),
            )
            for document in documents
        )
        books = tuple(
            replace(
                book,
                id=document.id,
                depends_on=document.depends_on,
            )
            for document, book in zip(documents, books, strict=True)
        )
    else:
        books = tuple(
            replace(
                book,
                source=_repo_relative(repo, book.source.as_posix(), name=f"books.{book.id}.source"),
            )
            for book in _read_books(
                raw_books, source_defaults=source_defaults, source_rules=source_rules
            )
        )
    chapters = tuple(chapter for book in books for chapter in _discover_chapters(repo, book))
    canonical_documents = None
    canonical_work_units = None
    if raw_books is None:
        canonical_documents = documents
        canonical_work_units = backend.map_units(resolved_sources.work_units)
        chapters = tuple(_chapter_from_work_unit(unit) for unit in canonical_work_units)
    elif set(raw_backend).intersection(
        {
            "root",
            "module",
            "path",
            "unit_module",
            "build_command",
            "scope",
            "target_root",
            "target_path",
            "target_module",
            "scope_templates",
            "build_command_template",
            "chapter_path",
            "chapter_module",
            "manifest",
            "mappings",
            "targets",
        }
    ):
        legacy_documents = {book.id: book.as_source_document() for book in books}
        units = tuple(
            chapter.as_work_unit(legacy_documents[chapter.book_id]) for chapter in chapters
        )
        canonical_documents = tuple(legacy_documents.values())
        canonical_work_units = backend.map_units(units)
        chapters = tuple(_chapter_from_work_unit(unit) for unit in canonical_work_units)
    config = PipelineConfig(
        path=config_path,
        settings=settings,
        stages=stages,
        shepherd=shepherd,
        books=books,
        chapters=chapters,
        source_rules=source_rules,
        source_roots=source_roots,
        source_include=source_include,
        source_exclude=source_exclude,
        backend=backend,
        canonical_documents=canonical_documents,
        canonical_work_units=canonical_work_units,
        project=project.bind(
            source_paths=(repo / book.source for book in books),
            target_dir=repo / backend.project,
            state_dir=state_dir,
        ),
    )
    # Validate the complete graph while loading, before any agents are launched.
    build_corpus_schedule(config.documents, config.work_units, phase="statements")
    return config


def _pascal_case(value: str) -> str:
    words = re.findall(r"[A-Za-z0-9]+", value)
    return "".join(word[:1].upper() + word[1:] for word in words) or "Formalization"


def _source_title(target: Path) -> str:
    source_format = format_for_path(target)
    if source_format == "markdown":
        return MarkdownAdapter(root=target.parent).read_document(target).title
    if source_format == "latex":
        return LatexAdapter(root=target.parent).read_document(target).title
    name = re.sub(r"^\d+[-_]", "", target.stem)
    return name.replace("-", " ").replace("_", " ").title()


def _existing_lean_stem(repo: Path, number: int) -> str | None:
    root = repo / "lean" / "LastLib"
    stems = {path.stem if path.is_file() else path.name for path in root.glob(f"Book{number:02d}*")}
    return next(iter(stems)) if len(stems) == 1 else None


def _infer_book(
    repo: Path,
    source_path: Path,
    *,
    options: dict[str, Any] | None = None,
    markdown_profile: str = "numbered-chapters",
) -> BookConfig:
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
    selected = options or {}
    source_format = str(selected.get("format") or format_for_path(source_path)).casefold()
    source_format = {
        "md": "markdown",
        "tex": "latex",
        "txt": "text",
        "plain-text": "text",
        "plaintext": "text",
    }.get(source_format, source_format)
    profile = str(
        selected.get("profile", markdown_profile if source_format == "markdown" else "default")
    )
    return BookConfig(
        id=book_id,
        title=title,
        source=source,
        lean_root=Path("lean") / "LastLib" / lean_stem,
        module=f"LastLib.{lean_stem}",
        format=source_format,
        adapter_profile=profile,
        unit=(
            str(selected["unit"])
            if "unit" in selected
            else "section"
            if source_format == "latex"
            else None
        ),
        follow_includes=bool(selected.get("follow_includes", False)),
        delimiter=str(selected["delimiter"]) if "delimiter" in selected else None,
        verbatim_environments=tuple(selected.get("verbatim_environments", ())),
        heading_pattern=(
            str(selected["heading_pattern"])
            if "heading_pattern" in selected
            else BookConfig.heading_pattern
            if source_format == "markdown" and profile == "numbered-chapters"
            else None
        ),
    )


def _inferred_book_from_document(repo: Path, document: Any, options: dict[str, Any]) -> BookConfig:
    book = _infer_book(
        repo,
        repo / document.path,
        options=options,
        markdown_profile="atx",
    )
    return replace(book, title=document.title)


def infer_config(target: str | Path, *, project: Project | None = None) -> PipelineConfig:
    source_path = Path(target).resolve()
    if source_path.is_dir():
        return infer_corpus((source_path,), project=project)
    if not source_path.is_file():
        raise ValueError(f"target must be an existing .md, .tex, or .txt file: {source_path}")
    try:
        format_for_path(source_path)
    except ValueError as error:
        raise ValueError(
            f"target must be an existing .md, .tex, or .txt file: {source_path}"
        ) from error
    project = project or ProjectResolver().resolve(targets=(source_path,))
    repo = project.root
    book = _infer_book(repo, source_path)
    settings = SwarmSettings(
        repo=repo,
        state_dir=repo / ".paf" / book.id,
        model="gpt-5.6-luna",
        reasoning_effort="xhigh",
    )
    chapters = tuple(_discover_chapters(repo, book))
    return PipelineConfig(
        path=source_path,
        settings=settings,
        stages=_stage_configs({}, repo),
        books=(book,),
        chapters=chapters,
        backend=LeanBackend(
            project=settings.lean_project,
            mcp_tool_timeout_seconds=settings.lean_mcp_tool_timeout_seconds,
        ),
        project=project.bind(
            root=repo,
            source_paths=(source_path,),
            target_dir=repo / settings.lean_project,
            state_dir=settings.state_dir,
        ),
    )


def _expand_source_targets(
    targets: tuple[str | Path, ...],
    *,
    project: Project | None = None,
) -> tuple[Path, tuple[Path, ...], frozenset[Path]]:
    if not targets:
        raise ValueError("corpus requires at least one source file or directory")
    supplied = tuple(Path(target).resolve() for target in targets)
    for path in supplied:
        if not path.exists():
            raise ValueError(f"source target does not exist: {path}")
    repo = (
        project.root
        if project is not None
        else ProjectResolver().resolve(targets=(supplied[0],)).root
    )
    resolver = SourceResolver(repo)
    relative = resolver.discover_paths(supplied)
    direct_files = frozenset(path for path in supplied if path.is_file())
    return repo, tuple(repo / path for path in relative), direct_files


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


def _inferred_target_backend(repo: Path, output_target: str | Path) -> tuple[LeanBackend, Path]:
    target = Path(output_target).expanduser()
    target = target.resolve() if target.is_absolute() else (Path.cwd() / target).resolve()
    try:
        target_root = target.relative_to(repo)
    except ValueError as error:
        raise ValueError("--target must be inside the inferred repository") from error
    if target_root == Path("."):
        raise ValueError("--target must name a directory inside the inferred repository")

    project = None
    for candidate in (target, *target.parents):
        if not candidate.is_relative_to(repo):
            break
        if (candidate / "lakefile.toml").is_file() or (candidate / "lakefile.lean").is_file():
            project = candidate
            break
        if candidate == repo:
            break
    if project is None:
        project = repo / target_root.parts[0]

    project_path = project.relative_to(repo)
    namespace_path = target.relative_to(project)
    namespace = ".".join(_pascal_case(part) for part in namespace_path.parts)
    if not namespace:
        namespace = _pascal_case(target.name)
    project_command = shlex.quote(project_path.as_posix())
    backend = LeanBackend(
        project=project_path,
        templates=TargetTemplates(
            root=target_root.as_posix(),
            module=namespace,
            build_command=f"cd {project_command} && lake build +{{unit_module}}",
        ),
    )
    return backend, target_root


def infer_corpus(
    targets: tuple[str | Path, ...],
    *,
    dependency_file: str | Path | None = None,
    output_target: str | Path | None = None,
    project: Project | None = None,
) -> PipelineConfig:
    project = project or ProjectResolver().resolve(targets=targets)
    repo, source_paths, direct_files = _expand_source_targets(targets, project=project)
    books = tuple(
        _infer_book(
            repo,
            source_path,
            markdown_profile=("numbered-chapters" if source_path in direct_files else "atx"),
        )
        for source_path in source_paths
    )
    ids = [book.id for book in books]
    if len(ids) != len(set(ids)):
        duplicates = {item for item in ids if ids.count(item) > 1}
        books = tuple(
            replace(
                book,
                id=(
                    re.sub(
                        r"[^a-z0-9]+",
                        "-",
                        book.source.with_suffix("").as_posix().casefold(),
                    ).strip("-")
                    if book.id in duplicates
                    else book.id
                ),
            )
            for book in books
        )
        if len({book.id for book in books}) != len(books):
            raise ValueError("inferred source document ids must be unique")

    graph_path = Path(dependency_file).resolve() if dependency_file is not None else None
    if graph_path is None and (repo / "BOOK_DEPENDENCIES.md").is_file():
        graph_path = repo / "BOOK_DEPENDENCIES.md"
    graph = parse_book_dependencies(graph_path) if graph_path is not None else {}
    selected = set(ids)
    books = tuple(
        replace(book, depends_on=tuple(item for item in graph.get(book.id, ()) if item in selected))
        for book in books
    )
    backend: LeanBackend
    if output_target is None:
        backend = LeanBackend()
    else:
        backend, target_root = _inferred_target_backend(repo, output_target)
        module_root = backend.templates.module
        project_command = shlex.quote(backend.project.as_posix())
        books = tuple(
            replace(
                book,
                lean_root=target_root / book.lean_root.name,
                module=f"{module_root}.{book.lean_root.name}",
                build_command=f"cd {project_command} && lake build +{{chapter_module}}",
            )
            for book in books
        )
    chapters = tuple(chapter for book in books for chapter in _discover_chapters(repo, book))
    identity = "\n".join(
        sorted(source_path.relative_to(repo).as_posix() for source_path in source_paths)
    )
    if output_target is not None:
        identity += f"\ntarget={backend.templates.root}"
    # Corpus IDs are durable state namespaces, not disposable cache keys. Keep
    # the original SHA-based identity stable across hashing implementations.
    corpus_id = stable_digest_text(identity)[:10]
    state_dir = repo / ".paf" / f"corpus-{corpus_id}"
    transitional_id = digest_text(identity)[:10]
    transitional_state_dir = repo / ".paf" / f"corpus-{transitional_id}"
    if not state_dir.exists() and transitional_state_dir.exists():
        # Preserve state created during the brief unversioned XXH transition.
        state_dir = transitional_state_dir
    settings = SwarmSettings(
        repo=repo,
        state_dir=state_dir,
        model="gpt-5.6-luna",
        reasoning_effort="xhigh",
        lean_project=backend.project,
    )
    config = PipelineConfig(
        path=graph_path or repo,
        settings=settings,
        stages=_stage_configs({}, repo),
        books=books,
        chapters=chapters,
        source_roots=tuple(Path(target).resolve().relative_to(repo) for target in targets),
        source_include=DEFAULT_INCLUDES,
        backend=replace(
            backend,
            mcp_tool_timeout_seconds=settings.lean_mcp_tool_timeout_seconds,
        ),
        project=project.bind(
            root=repo,
            source_paths=source_paths,
            target_dir=repo / settings.lean_project,
            state_dir=settings.state_dir,
        ),
    )
    build_corpus_schedule(config.documents, config.work_units, phase="statements")
    return config


def resolve_config(
    *,
    config: str | Path | None,
    target: str | Path | None,
    dependency_file: str | Path | None = None,
    project: Project | None = None,
) -> PipelineConfig:
    project = project or ProjectResolver().resolve(
        targets=((target,) if target is not None else ()), config=config
    )
    if config is not None and target is not None:
        raise ValueError("pass either --config or a source target, not both")
    if config is not None:
        if dependency_file is not None:
            raise ValueError("--dependencies is only used with inferred source targets")
        return load_config(config, project=project)
    if target is not None:
        path = Path(target)
        if path.is_dir():
            return infer_corpus((path,), dependency_file=dependency_file, project=project)
        if dependency_file is not None:
            return infer_corpus((path,), dependency_file=dependency_file, project=project)
        return infer_config(path, project=project)
    default = project.config_path
    if default is not None and default.is_file():
        return load_config(default, project=project)
    raise ValueError("pass a .md, .tex, or .txt target or --config; no paf.toml was found")
