from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import subprocess
from pathlib import Path
from typing import Any


def search_lean_sources(query: str, *, root: Path, limit: int = 32) -> list[dict[str, Any]]:
    """Search project, dependency, and toolchain Lean sources without an LSP server."""

    if limit < 1:
        raise ValueError("search limit must be positive")
    ripgrep = shutil.which("rg")
    if ripgrep is None:
        raise RuntimeError("ripgrep (`rg`) is required for `paf lean search`")

    matches: list[dict[str, Any]] = []
    for source_root in _source_roots(root.resolve()):
        command = [
            ripgrep,
            "--json",
            "--line-number",
            "--smart-case",
            "--fixed-strings",
            "--glob",
            "*.lean",
            "--glob",
            "!.git/**",
            "--glob",
            "!.lake/build/**",
            "--glob",
            "!.lake/packages/**",
            "--",
            query,
            str(source_root),
        ]
        completed = subprocess.run(command, check=False, capture_output=True, text=True)
        if completed.returncode not in {0, 1}:
            raise RuntimeError(completed.stderr.strip() or "Lean source search failed")
        for line in completed.stdout.splitlines():
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if event.get("type") != "match" or not isinstance(event.get("data"), dict):
                continue
            data = event["data"]
            path = data.get("path", {}).get("text")
            text = data.get("lines", {}).get("text")
            line_number = data.get("line_number")
            if not isinstance(path, str) or not isinstance(text, str):
                continue
            matches.append(
                {
                    "path": _display_path(Path(path), root),
                    "line": line_number,
                    "text": text.rstrip(),
                    "source": _source_label(Path(path), root),
                }
            )
            if len(matches) >= limit:
                return matches
        if len(matches) >= limit:
            return matches
    return matches


def prepare_lean_dependencies(
    files: list[str],
    *,
    root: Path,
    beam_command: str | None = None,
    max_rounds: int = 8,
) -> dict[str, Any]:
    """Follow Beam's stale-direct-import recovery until targets synchronize."""

    if not files:
        raise ValueError("at least one Lean file is required")
    if max_rounds < 1:
        raise ValueError("max rounds must be positive")
    requested_command = beam_command or os.environ.get("PAF_BEAM_COMMAND", "lean-beam")
    executable = shutil.which(requested_command)
    if executable is None and Path(requested_command).is_file():
        executable = str(Path(requested_command).resolve())
    if executable is None:
        raise RuntimeError(f"Lean Beam executable was not found: {requested_command}")

    root = root.resolve()
    prepared: list[str] = []
    responses: dict[str, Any] = {}
    failure: tuple[str, Any] | None = None

    def checkpoint(file: str, stack: tuple[str, ...]) -> bool:
        nonlocal failure
        if file in stack:
            failure = (file, {"error": f"cyclic Beam recovery plan: {' -> '.join((*stack, file))}"})
            return False
        for attempt in range(max_rounds):
            operation = "save" if attempt == 0 else "refresh"
            response, success = _beam_json(executable, root, operation, file)
            responses[f"{operation}:{file}"] = response
            if success:
                if operation == "refresh":
                    response, success = _beam_json(executable, root, "save", file)
                    responses[f"save:{file}"] = response
                if success:
                    return True
            dependencies = _recovery_files(response)
            if not dependencies:
                if _recovery_requests_refresh(response, file):
                    continue
                failure = (file, response)
                return False
            if not all(checkpoint(dependency, (*stack, file)) for dependency in dependencies):
                return False
        failure = (file, {"error": f"Beam checkpoint did not converge after {max_rounds} rounds"})
        return False

    for file in dict.fromkeys(files):
        response, success = _beam_json(executable, root, "sync", file)
        responses[f"sync:{file}"] = response
        for _ in range(max_rounds):
            if success:
                prepared.append(file)
                break
            dependencies = _recovery_files(response)
            if dependencies:
                if not all(checkpoint(dependency, (file,)) for dependency in dependencies):
                    break
            elif not _recovery_requests_refresh(response, file):
                failure = (file, response)
                break
            response, success = _beam_json(executable, root, "refresh", file)
            responses[f"refresh:{file}"] = response
        else:
            failure = (file, {"error": f"Beam sync did not converge after {max_rounds} rounds"})
        if failure is not None or file not in prepared:
            blocked, response = failure or (file, response)
            return {
                "ok": False,
                "prepared": prepared,
                "blocked": blocked,
                "response": response,
                "responses": responses,
            }
    return {"ok": True, "prepared": prepared, "responses": responses}


def _beam_json(executable: str, root: Path, operation: str, file: str) -> tuple[Any, bool]:
    completed = subprocess.run(
        [executable, "--root", str(root), operation, file],
        cwd=root,
        env=os.environ.copy(),
        check=False,
        capture_output=True,
        text=True,
    )
    output = completed.stdout.strip() or completed.stderr.strip()
    try:
        response: Any = json.loads(output)
    except json.JSONDecodeError:
        response = {"error": output or f"Lean Beam exited with status {completed.returncode}"}
    has_error = isinstance(response, dict) and response.get("error") is not None
    return response, completed.returncode == 0 and not has_error


def _recovery_files(value: Any) -> list[str]:
    """Extract only Lean paths from Beam's structured recovery fields."""

    found: list[str] = []

    def visit(item: Any, *, recovery: bool = False) -> None:
        if isinstance(item, dict):
            for key, child in item.items():
                visit(child, recovery=recovery or key in {"saveDeps", "staleDirectDeps"})
        elif isinstance(item, list):
            for child in item:
                visit(child, recovery=recovery)
        elif recovery and isinstance(item, str):
            found.extend(re.findall(r"[^\s'\"`]+\.lean", item))

    visit(value)
    return list(dict.fromkeys(found))


def _recovery_requests_refresh(value: Any, file: str) -> bool:
    """Whether Beam's structured recovery plan asks to refresh this target."""

    plans: list[str] = []

    def visit(item: Any) -> None:
        if isinstance(item, dict):
            for key, child in item.items():
                if key == "recoveryPlan" and isinstance(child, list):
                    plans.extend(step for step in child if isinstance(step, str))
                else:
                    visit(child)
        elif isinstance(item, list):
            for child in item:
                visit(child)

    visit(value)
    for plan in plans:
        try:
            arguments = shlex.split(plan)
        except ValueError:
            continue
        for index, argument in enumerate(arguments[:-1]):
            if argument == "refresh" and arguments[index + 1] == file:
                return True
    return False


def _source_roots(root: Path) -> tuple[Path, ...]:
    candidates = [root]
    packages = root / ".lake" / "packages"
    if packages.is_dir():
        candidates.extend(path for path in sorted(packages.iterdir()) if path.is_dir())
    lean = shutil.which("lean")
    if lean is not None:
        prefix = subprocess.run(
            [lean, "--print-prefix"], check=False, capture_output=True, text=True
        ).stdout.strip()
        if prefix:
            candidates.extend((Path(prefix) / "src" / "lean", Path(prefix) / "src"))
    return tuple(dict.fromkeys(path.resolve() for path in candidates if path.is_dir()))


def _display_path(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return str(path)


def _source_label(path: Path, root: Path) -> str:
    resolved = path.resolve()
    try:
        relative = resolved.relative_to(root.resolve())
    except ValueError:
        return "toolchain"
    return "dependency" if relative.parts[:2] == (".lake", "packages") else "project"
