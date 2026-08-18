from __future__ import annotations

import os
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path
from typing import Any

from paf import json_codec as json
from paf.hashing import ALGORITHM, digest_file, stable_digest_fields
from paf.models import PipelineConfig, WorkUnitLike
from paf.scope import ScopeMatcher

FINGERPRINT_SCHEMA = "olean-proof-erased-v1"
_HELPER_BATCH_SIZE = 512


class InterfaceFingerprintError(RuntimeError):
    """The compiled Lean interface could not be fingerprinted safely."""


@dataclass(frozen=True)
class ModuleFingerprint:
    module: str
    source: str
    artifact: str
    imports: tuple[str, ...]
    artifact_digest: str
    interface_digest: str
    declaration_count: int

    def as_dict(self) -> dict[str, object]:
        return {
            "module": self.module,
            "source": self.source,
            "artifact": self.artifact,
            "imports": list(self.imports),
            "artifact_digest": self.artifact_digest,
            "interface_digest": self.interface_digest,
            "declaration_count": self.declaration_count,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> ModuleFingerprint:
        imports = value.get("imports", ())
        if not isinstance(imports, list) or not all(isinstance(item, str) for item in imports):
            raise InterfaceFingerprintError("cached module imports are invalid")
        return cls(
            module=str(value["module"]),
            source=str(value["source"]),
            artifact=str(value["artifact"]),
            imports=tuple(imports),
            artifact_digest=str(value["artifact_digest"]),
            interface_digest=str(value["interface_digest"]),
            declaration_count=int(value["declaration_count"]),
        )


@dataclass(frozen=True)
class WorkUnitFingerprint:
    work_unit_id: str
    modules: tuple[ModuleFingerprint, ...]
    artifact_digest: str
    interface_digest: str
    fingerprint_schema: str
    lean_version: str

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_digest": self.artifact_digest,
            "interface_digest": self.interface_digest,
            "fingerprint_schema": self.fingerprint_schema,
            "lean_version": self.lean_version,
            "modules": [module.as_dict() for module in self.modules],
        }


@dataclass(frozen=True)
class FingerprintCollection:
    records: dict[str, WorkUnitFingerprint]
    module_owners: dict[str, str]
    dependencies: dict[str, frozenset[str]]
    lean_version: str


def aggregate_module_digests(
    modules: tuple[ModuleFingerprint, ...],
    *,
    lean_version: str,
) -> tuple[str, str]:
    ordered = tuple(sorted(modules, key=lambda item: item.module))
    artifact = stable_digest_fields(
        "paf-work-unit-artifact-v1",
        FINGERPRINT_SCHEMA,
        lean_version,
        *(value for item in ordered for value in (item.module, item.artifact_digest)),
    )
    interface = stable_digest_fields(
        "paf-work-unit-interface-v1",
        FINGERPRINT_SCHEMA,
        lean_version,
        *(value for item in ordered for value in (item.module, item.interface_digest)),
    )
    return artifact, interface


def _lake_executable() -> str:
    if lake := shutil.which("lake"):
        return lake
    candidate = Path.home() / ".elan" / "bin" / "lake"
    if candidate.is_file():
        return str(candidate)
    raise InterfaceFingerprintError("lake was not found on PATH or in ~/.elan/bin")


def _module_name(project: Path, source: Path) -> str:
    try:
        relative = source.resolve().relative_to(project.resolve())
    except ValueError as error:
        raise InterfaceFingerprintError(
            f"Lean source is outside the configured project: {source}"
        ) from error
    if relative.suffix != ".lean":
        raise InterfaceFingerprintError(f"expected a .lean source path: {source}")
    return ".".join(relative.with_suffix("").parts)


def _olean_path(project: Path, module: str) -> Path | None:
    relative = Path(*module.split(".")).with_suffix(".olean")
    for root in (
        project / ".lake" / "build" / "lib" / "lean",
        project / ".lake" / "build" / "lib",
    ):
        candidate = root / relative
        if candidate.is_file():
            return candidate
    return None


def _artifact_digest(path: Path) -> str:
    hash_path = Path(str(path) + ".hash")
    if hash_path.is_file():
        value = hash_path.read_text(encoding="ascii").strip()
        if value:
            return f"lake:{value}"
    return f"{ALGORITHM}:{digest_file(path)}"


def _helper_path() -> Path:
    path = Path(str(files("paf").joinpath("lean/InterfaceFingerprint.lean")))
    if not path.is_file():
        raise InterfaceFingerprintError("packaged Lean interface helper is missing")
    return path


def _helper_environment() -> dict[str, str]:
    environment = dict(os.environ)
    elan = str(Path.home() / ".elan" / "bin")
    environment["PATH"] = os.pathsep.join(
        dict.fromkeys((str(Path(_lake_executable()).parent), elan, environment.get("PATH", "")))
    )
    return environment


def _lean_identity(project: Path, *, timeout_seconds: float) -> str:
    try:
        completed = subprocess.run(
            [_lake_executable(), "env", "lean", "--version"],
            cwd=project,
            env=_helper_environment(),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        raise InterfaceFingerprintError("reading the Lean version timed out") from error
    match = re.search(r"version ([^,\s]+).*?commit ([0-9a-f]+)", completed.stdout)
    if completed.returncode != 0 or match is None:
        raise InterfaceFingerprintError("could not determine the target project's Lean version")
    return f"{match.group(1)}:{match.group(2)}"


def _run_helper(
    project: Path,
    requests: tuple[tuple[str, Path, Path], ...],
    *,
    timeout_seconds: float,
) -> tuple[dict[str, Any], ...]:
    command = [_lake_executable(), "env", "lean", "--run", str(_helper_path())]
    for module, source, output in requests:
        command.extend((module, str(source), str(output)))
    try:
        completed = subprocess.run(
            command,
            cwd=project,
            env=_helper_environment(),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        raise InterfaceFingerprintError("Lean interface fingerprint helper timed out") from error
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    if completed.returncode != 0 or not lines:
        detail = completed.stdout[-4000:] or f"helper exited with status {completed.returncode}"
        raise InterfaceFingerprintError(detail)
    try:
        payload = json.loads(lines[-1])
    except ValueError as error:
        raise InterfaceFingerprintError(
            "Lean interface fingerprint helper returned invalid JSON:\n" + completed.stdout[-4000:]
        ) from error
    if not isinstance(payload, list) or not all(isinstance(item, dict) for item in payload):
        raise InterfaceFingerprintError("Lean interface fingerprint helper returned a non-array")
    return tuple(payload)


def _owned_sources(
    root: Path,
    lean_project: Path,
    work_units: tuple[WorkUnitLike, ...],
) -> tuple[dict[str, tuple[tuple[str, Path], ...]], dict[str, str]]:
    by_owner: dict[str, tuple[tuple[str, Path], ...]] = {}
    module_owners: dict[str, str] = {}
    for work_unit in work_units:
        values: list[tuple[str, Path]] = []
        for source in ScopeMatcher(work_unit.scope).files(root):
            if source.suffix != ".lean":
                continue
            module = _module_name(lean_project, source)
            previous = module_owners.setdefault(module, work_unit.id)
            if previous != work_unit.id:
                raise InterfaceFingerprintError(
                    f"Lean module {module} is owned by both {previous} and {work_unit.id}"
                )
            values.append((module, source))
        by_owner[work_unit.id] = tuple(sorted(values))
    return by_owner, module_owners


def collect_interface_fingerprints(
    config: PipelineConfig,
    work_units: tuple[WorkUnitLike, ...],
    *,
    root: Path | None = None,
    cached_records: dict[str, Any] | None = None,
) -> FingerprintCollection:
    """Fingerprint every compiled module owned by ``work_units``.

    The helper runs under the target project's exact Lake/Lean toolchain.  One
    invocation handles hundreds of modules so collection remains cheap compared
    with the coordinator build that produced the artifacts.
    """

    repository = (root or config.settings.repo).resolve()
    lean_project = repository / config.settings.lean_project
    sources_by_owner, module_owners = _owned_sources(repository, lean_project, work_units)
    artifacts: dict[str, tuple[Path, Path]] = {}
    for values in sources_by_owner.values():
        for module, source in values:
            artifact = _olean_path(lean_project, module)
            if artifact is None:
                raise InterfaceFingerprintError(f"compiled artifact is missing for {module}")
            artifacts[module] = (source, artifact)
    lean_version = _lean_identity(
        lean_project,
        timeout_seconds=config.settings.validation_timeout_seconds,
    )

    helper_values: dict[str, dict[str, Any]] = {}
    serialized: dict[str, bytes] = {}
    cached_modules: dict[str, ModuleFingerprint] = {}
    raw_cache = cached_records or {}
    for work_unit in work_units:
        record = raw_cache.get(work_unit.id)
        if (
            not isinstance(record, dict)
            or record.get("fingerprint_schema") != FINGERPRINT_SCHEMA
            or record.get("lean_version") != lean_version
            or not isinstance(record.get("modules"), list)
        ):
            continue
        for value in record["modules"]:
            if not isinstance(value, dict):
                continue
            try:
                module = ModuleFingerprint.from_dict(value)
            except (KeyError, TypeError, ValueError, InterfaceFingerprintError):
                continue
            artifact = artifacts.get(module.module)
            if artifact is not None and module.artifact_digest == _artifact_digest(artifact[1]):
                cached_modules[module.module] = module

    with tempfile.TemporaryDirectory(prefix="paf-interface-") as temporary:
        temporary_root = Path(temporary)
        modules = tuple(sorted(set(artifacts).difference(cached_modules)))
        for offset in range(0, len(modules), _HELPER_BATCH_SIZE):
            batch = modules[offset : offset + _HELPER_BATCH_SIZE]
            requests = tuple(
                (
                    module,
                    artifacts[module][1],
                    temporary_root / f"{offset + index}.olean",
                )
                for index, module in enumerate(batch)
            )
            values = _run_helper(
                lean_project,
                requests,
                timeout_seconds=config.settings.validation_timeout_seconds,
            )
            if len(values) != len(requests):
                raise InterfaceFingerprintError("Lean helper returned the wrong result count")
            for request, value in zip(requests, values, strict=True):
                module, _, output = request
                if value.get("module") != module or not output.is_file():
                    raise InterfaceFingerprintError(f"Lean helper omitted output for {module}")
                helper_values[module] = value
                serialized[module] = output.read_bytes()

    versions = {
        f"{value.get('lean_version', '')}:{value.get('lean_githash', '')}"
        for value in helper_values.values()
    }
    if len(versions) > 1 or (versions and versions != {lean_version}):
        raise InterfaceFingerprintError("Lean helper returned inconsistent toolchain versions")
    modules_by_owner: dict[str, list[ModuleFingerprint]] = {
        work_unit.id: [] for work_unit in work_units
    }
    for module in sorted(artifacts):
        source, artifact = artifacts[module]
        if cached := cached_modules.get(module):
            modules_by_owner[module_owners[module]].append(cached)
            continue
        value = helper_values[module]
        interface_digest = stable_digest_fields(
            "paf-module-interface-v1",
            FINGERPRINT_SCHEMA,
            lean_version,
            module,
            serialized[module],
        )
        imports = tuple(dict.fromkeys(str(item) for item in value.get("imports", ())))
        modules_by_owner[module_owners[module]].append(
            ModuleFingerprint(
                module=module,
                source=source.relative_to(repository).as_posix(),
                artifact=artifact.relative_to(repository).as_posix(),
                imports=imports,
                artifact_digest=_artifact_digest(artifact),
                interface_digest=interface_digest,
                declaration_count=int(value.get("declaration_count", 0)),
            )
        )

    records: dict[str, WorkUnitFingerprint] = {}
    dependencies = {work_unit.id: set[str]() for work_unit in work_units}
    for work_unit in work_units:
        modules = tuple(sorted(modules_by_owner[work_unit.id], key=lambda item: item.module))
        if not modules:
            raise InterfaceFingerprintError(
                f"work unit {work_unit.id} owns no compiled Lean modules"
            )
        artifact_digest, interface_digest = aggregate_module_digests(
            modules,
            lean_version=lean_version,
        )
        records[work_unit.id] = WorkUnitFingerprint(
            work_unit_id=work_unit.id,
            modules=modules,
            artifact_digest=artifact_digest,
            interface_digest=interface_digest,
            fingerprint_schema=FINGERPRINT_SCHEMA,
            lean_version=lean_version,
        )
        for module in modules:
            for imported in module.imports:
                owner = module_owners.get(imported)
                if owner is not None and owner != work_unit.id:
                    dependencies[work_unit.id].add(owner)

    return FingerprintCollection(
        records=records,
        module_owners=module_owners,
        dependencies={key: frozenset(value) for key, value in dependencies.items()},
        lean_version=lean_version,
    )
