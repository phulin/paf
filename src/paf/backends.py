from __future__ import annotations

import json
import os
import re
import shutil
import sys
import tomllib
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from paf.diagnostics import lean_diagnostic_counts
from paf.models import Stage, TargetMapping, WorkUnit


@runtime_checkable
class TargetBackend(Protocol):
    """Language/toolchain boundary used by planning and orchestration."""

    kind: str
    project: Path

    def map_units(self, units: Iterable[WorkUnit]) -> tuple[WorkUnit, ...]: ...

    def scaffold_paths(self, unit: WorkUnit) -> tuple[Path, ...]: ...

    def diagnostic_counts(self, output: str) -> tuple[int, int]: ...

    def mcp_config(self, root: Path, stage: Stage) -> dict[str, Any]: ...


LEAN_MCP_BASE_TOOLS = (
    "lean_diagnostic_messages",
    "lean_prepare_dependencies",
    "lean_hover_info",
    "lean_declaration_file",
    "lean_local_search",
)
LEAN_MCP_PROOF_TOOLS = (
    *LEAN_MCP_BASE_TOOLS,
    "lean_goal",
    "lean_term_goal",
    "lean_completions",
    "lean_multi_attempt",
    "lean_code_actions",
)
LEAN_MCP_FIXUP_TOOLS = (*LEAN_MCP_BASE_TOOLS, "lean_completions", "lean_code_actions")


def _module_component(value: str) -> str:
    words = re.findall(r"[^\W_]+", value, flags=re.UNICODE)
    result = "".join(word[:1].upper() + word[1:] for word in words)
    if not result:
        return "Source"
    return f"N{result}" if result[0].isdigit() else result


def target_variables(unit: WorkUnit) -> dict[str, str]:
    source_stem = unit.source.with_suffix("")
    document_module = ".".join(_module_component(part) for part in source_stem.parts)
    return {
        "work_unit_id": unit.id,
        "document_id": unit.document_id,
        "document_title": unit.document.title,
        "document_format": unit.document.format,
        "document_path": unit.source.as_posix(),
        "document_stem": unit.source.stem,
        "document_module": document_module,
        "source": unit.source.as_posix(),
        "source_name": unit.source.name,
        "source_stem": unit.source.stem,
        "source_path_stem": source_stem.as_posix(),
        "source_dir": unit.source.parent.as_posix(),
        "source_start_line": str(unit.source_span.start_line),
        "source_end_line": str(unit.source_span.end_line),
        "unit_ordinal": str(unit.ordinal),
        "unit_ordinal_padded": f"{unit.ordinal:02d}",
        "unit_title": unit.title,
        # Legacy template spellings remain valid for [[books]] migrations.
        "book_id": unit.document_id,
        "book_title": unit.document.title,
        "chapter_number": str(unit.ordinal),
        "chapter_number_padded": f"{unit.ordinal:02d}",
        "chapter_title": unit.title,
        **unit.context,
    }


def render_target_template(template: str, variables: Mapping[str, str], *, name: str) -> str:
    rendered = template
    for key, value in variables.items():
        rendered = rendered.replace("{" + key + "}", value)
    unresolved = sorted(set(re.findall(r"\{([A-Za-z_][A-Za-z0-9_]*)\}", rendered)))
    if unresolved:
        raise ValueError(f"{name} references unknown template variables: {', '.join(unresolved)}")
    return rendered


@dataclass(frozen=True)
class TargetTemplates:
    root: str = "lean/Formalization"
    module: str = "Formalization"
    path: str = "{document_module}/Unit{unit_ordinal_padded}"
    unit_module: str = "{module}.{document_module}.Unit{unit_ordinal_padded}"
    build_command: str = "cd lean && lake build +{unit_module}"
    scope: tuple[str, ...] = (
        "{root}/{path}.lean",
        "{root}/{path}/**/*.lean",
    )


@dataclass(frozen=True)
class ExplicitTarget:
    work_unit: str
    values: dict[str, Any]


@dataclass(frozen=True)
class LeanBackend:
    project: Path = Path("lean")
    templates: TargetTemplates = field(default_factory=TargetTemplates)
    explicit: tuple[ExplicitTarget, ...] = ()
    mcp_enabled: bool = True
    mcp_tool_timeout_seconds: float = 300.0
    kind: str = "lean"

    def __post_init__(self) -> None:
        if self.project.is_absolute():
            raise ValueError("backend.project must be repository-relative")
        if self.mcp_tool_timeout_seconds <= 0:
            raise ValueError("backend.mcp_tool_timeout_seconds must be positive")
        identifiers = [item.work_unit for item in self.explicit]
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("backend explicit work-unit mappings must be unique")

    def _mapping(self, unit: WorkUnit) -> TargetMapping:
        values: dict[str, Any] = {
            "root": self.templates.root,
            "module": self.templates.module,
            "path": self.templates.path,
            "unit_module": self.templates.unit_module,
            "build_command": self.templates.build_command,
            "scope": self.templates.scope,
        }
        explicit = next((item.values for item in self.explicit if item.work_unit == unit.id), None)
        if explicit is not None:
            values.update(explicit)
        variables = target_variables(unit)
        root = render_target_template(str(values["root"]), variables, name="backend.root")
        variables["root"] = root
        variables["lean_root"] = root
        module = render_target_template(str(values["module"]), variables, name="backend.module")
        variables["module"] = module
        path = render_target_template(str(values["path"]), variables, name="backend.path")
        variables["path"] = path
        variables["chapter_path"] = path
        unit_module = render_target_template(
            str(values["unit_module"]), variables, name="backend.unit_module"
        )
        variables["unit_module"] = unit_module
        variables["chapter_module"] = unit_module
        build_command = render_target_template(
            str(values["build_command"]), variables, name="backend.build_command"
        )
        variables["build_command"] = build_command
        raw_scope = values["scope"]
        if isinstance(raw_scope, str) or not isinstance(raw_scope, Sequence):
            raise ValueError("backend.scope must be a list of strings")
        scope = tuple(
            render_target_template(str(item), variables, name="backend.scope") for item in raw_scope
        )
        return TargetMapping(
            backend=self.kind,
            root=Path(root),
            module=module,
            path=path,
            unit_module=unit_module,
            build_command=build_command,
            scope=scope,
        )

    def map_units(self, units: Iterable[WorkUnit]) -> tuple[WorkUnit, ...]:
        result = tuple(replace(unit, target=self._mapping(unit)) for unit in units)
        known = {unit.id for unit in result}
        unknown = [item.work_unit for item in self.explicit if item.work_unit not in known]
        if unknown:
            raise ValueError(
                "backend mappings reference unknown work units: " + ", ".join(sorted(unknown))
            )
        return result

    def scaffold_paths(self, unit: WorkUnit) -> tuple[Path, ...]:
        target = unit._target()
        base = target.root / target.path
        # A file-shaped explicit mapping scaffolds its parent; the usual stem-shaped
        # mapping scaffolds the declaration directory, matching legacy behavior.
        return (base.parent if base.suffix else base,)

    def diagnostic_counts(self, output: str) -> tuple[int, int]:
        return lean_diagnostic_counts(output)

    @staticmethod
    def _mcp_path() -> str:
        current = os.environ.get("PATH", "").split(os.pathsep)
        executable = Path(sys.executable)
        candidates = [executable.parent]
        if elan_home := os.environ.get("ELAN_HOME"):
            candidates.append(Path(elan_home) / "bin")
        candidates.append(Path.home() / ".elan" / "bin")
        if lake := shutil.which("lake"):
            candidates.append(Path(lake).parent)
        prefixes = [str(path) for path in candidates if (path / "lake").is_file()]
        return os.pathsep.join(dict.fromkeys([*prefixes, *current]))

    def mcp_config(self, root: Path, stage: Stage) -> dict[str, Any]:
        if not self.mcp_enabled or stage not in (Stage.FORMALIZE, Stage.REVIEW, Stage.PROVE):
            return {}
        project = (root / self.project).resolve()
        tools = (
            LEAN_MCP_FIXUP_TOOLS
            if stage in (Stage.FORMALIZE, Stage.REVIEW)
            else LEAN_MCP_PROOF_TOOLS
        )
        return {
            "mcp_servers.paf_lean.command": str(Path(sys.executable)),
            "mcp_servers.paf_lean.args": ["-m", "paf.lean_mcp"],
            "mcp_servers.paf_lean.cwd": str(project),
            "mcp_servers.paf_lean.required": True,
            "mcp_servers.paf_lean.startup_timeout_sec": 60,
            "mcp_servers.paf_lean.tool_timeout_sec": self.mcp_tool_timeout_seconds,
            "mcp_servers.paf_lean.default_tools_approval_mode": "auto",
            "mcp_servers.paf_lean.enabled_tools": list(tools),
            "mcp_servers.paf_lean.env.PATH": self._mcp_path(),
            "mcp_servers.paf_lean.env.LEAN_PROJECT_PATH": str(project),
            "mcp_servers.paf_lean.env.LEAN_MCP_SCRATCH_SLOTS": "1",
            "mcp_servers.paf_lean.env.LEAN_LOG_LEVEL": "NONE",
            "mcp_servers.paf_lean.env.PYTHONWARNINGS": "ignore",
        }


_TARGET_KEYS = {"root", "module", "path", "unit_module", "build_command", "scope"}
_TARGET_ALIASES = {
    "target_root": "root",
    "target_path": "path",
    "target_module": "unit_module",
    "scope_templates": "scope",
    "build_command_template": "build_command",
    "chapter_path": "path",
    "chapter_module": "unit_module",
}


def _normalized_target_values(value: Mapping[str, Any]) -> dict[str, Any]:
    normalized = {key: item for key, item in value.items() if key in _TARGET_KEYS}
    for alias, key in _TARGET_ALIASES.items():
        if alias in value:
            if key in normalized:
                raise ValueError(f"configure backend.{key} or backend.{alias}, not both")
            normalized[key] = value[alias]
    for key in _TARGET_KEYS - {"scope"}:
        if key in normalized and (not isinstance(normalized[key], str) or not normalized[key]):
            raise ValueError(f"backend.{key} must be a non-empty string")
    if "scope" in normalized:
        scope = normalized["scope"]
        if (
            isinstance(scope, str)
            or not isinstance(scope, list | tuple)
            or not all(isinstance(item, str) and item for item in scope)
        ):
            raise ValueError("backend.scope must be a list of non-empty strings")
    return normalized


def _manifest_entries(path: Path) -> list[dict[str, Any]]:
    try:
        if path.suffix.casefold() == ".json":
            value = json.loads(path.read_text(encoding="utf-8"))
        else:
            with path.open("rb") as handle:
                raw = tomllib.load(handle)
            value = raw.get("targets", raw.get("mappings", raw))
    except (OSError, ValueError, tomllib.TOMLDecodeError) as error:
        raise ValueError(f"cannot read backend manifest: {path}") from error
    if isinstance(value, dict):
        value = [dict(mapping, work_unit=work_unit) for work_unit, mapping in value.items()]
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise ValueError("backend manifest must contain a list or table of work-unit mappings")
    return [dict(item) for item in value]


def lean_backend_from_config(
    raw: Mapping[str, Any],
    *,
    repo: Path,
    legacy_project: Path,
    legacy_timeout: float,
) -> LeanBackend:
    kind = str(raw.get("kind", raw.get("type", "lean"))).casefold()
    if kind != "lean":
        raise ValueError(f"unsupported backend kind: {kind}")
    allowed = {
        "kind",
        "type",
        "project",
        "manifest",
        "mappings",
        "targets",
        "mcp_enabled",
        "mcp_tool_timeout_seconds",
        *_TARGET_KEYS,
        *_TARGET_ALIASES,
    }
    unknown = set(raw) - allowed
    if unknown:
        raise ValueError(f"unknown backend keys: {', '.join(sorted(unknown))}")
    if "project" in raw and (not isinstance(raw["project"], str) or not raw["project"]):
        raise ValueError("backend.project must be a non-empty string")
    if "mcp_enabled" in raw and not isinstance(raw["mcp_enabled"], bool):
        raise ValueError("backend.mcp_enabled must be a boolean")
    template_values = _normalized_target_values(raw)
    scope = template_values.get("scope", TargetTemplates.scope)
    templates = TargetTemplates(
        root=str(template_values.get("root", TargetTemplates.root)),
        module=str(template_values.get("module", TargetTemplates.module)),
        path=str(template_values.get("path", TargetTemplates.path)),
        unit_module=str(template_values.get("unit_module", TargetTemplates.unit_module)),
        build_command=str(template_values.get("build_command", TargetTemplates.build_command)),
        scope=tuple(scope),
    )
    entries: list[dict[str, Any]] = []
    manifest = raw.get("manifest")
    if manifest is not None:
        if not isinstance(manifest, str) or not manifest:
            raise ValueError("backend.manifest must be a non-empty path")
        manifest_path = Path(manifest)
        if not manifest_path.is_absolute():
            manifest_path = repo / manifest_path
        entries.extend(_manifest_entries(manifest_path))
    configured = raw.get("mappings", raw.get("targets", []))
    if not isinstance(configured, list):
        raise ValueError("backend.mappings must be an array of tables")
    if not all(isinstance(item, dict) for item in configured):
        raise ValueError("each backend mapping must be a table")
    entries.extend(dict(item) for item in configured)
    explicit: list[ExplicitTarget] = []
    for entry in entries:
        identifier = entry.get("work_unit", entry.get("id"))
        if not isinstance(identifier, str) or not identifier:
            raise ValueError("each backend mapping requires work_unit")
        values = _normalized_target_values(entry)
        unknown_entry = set(entry) - _TARGET_KEYS - set(_TARGET_ALIASES) - {"work_unit", "id"}
        if unknown_entry:
            raise ValueError(f"unknown backend mapping keys: {', '.join(sorted(unknown_entry))}")
        explicit.append(ExplicitTarget(identifier, values))
    project_value = Path(str(raw.get("project", legacy_project.as_posix())))
    resolved = (
        project_value.resolve() if project_value.is_absolute() else (repo / project_value).resolve()
    )
    try:
        project = resolved.relative_to(repo)
    except ValueError as error:
        raise ValueError("backend.project must be inside swarm.repo") from error
    return LeanBackend(
        project=project,
        templates=templates,
        explicit=tuple(explicit),
        mcp_enabled=bool(raw.get("mcp_enabled", True)),
        mcp_tool_timeout_seconds=float(raw.get("mcp_tool_timeout_seconds", legacy_timeout)),
    )
