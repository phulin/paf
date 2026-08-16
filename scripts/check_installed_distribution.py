#!/usr/bin/env python3
"""Build and exercise PAF's wheel and sdist outside the source checkout.

Each archive is installed into its own temporary virtual environment. All probes
run with an unrelated working directory, no ``PYTHONPATH``, and user site
packages disabled so a successful result cannot be supplied by ``src/paf``.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

from check_distribution import check_archive

RESOURCE_PROBE = r"""
import asyncio
import json
import sys
from importlib.resources import files
from pathlib import Path

import paf
from paf.codex import COMMON_PROMPT_PATH, PROOF_REVIEW_PROMPT_PATH
from paf.config import load_config, standard_prompt_path
from paf.models import Stage
from paf.state import StateStore

project, external_state, checkout, environment = map(Path, sys.argv[1:])
package = files("paf")
prompts = files("paf.prompts")
web = package.joinpath("web_dist")
origin = Path(paf.__file__).resolve()
assert origin.is_relative_to(environment.resolve()), origin
assert not origin.is_relative_to(checkout.resolve()), origin
for stage in Stage:
    expected = prompts.joinpath(f"{stage.value}.md")
    actual = standard_prompt_path(stage)
    assert actual == Path(str(expected)) and actual.is_file(), (stage, actual, expected)
for actual, name in (
    (COMMON_PROMPT_PATH, "common.md"),
    (PROOF_REVIEW_PROMPT_PATH, "proof_review.md"),
):
    assert actual == Path(str(prompts.joinpath(name))) and actual.is_file(), actual
index = Path(str(web.joinpath("index.html")))
manifest_path = Path(str(web.joinpath("bundle-manifest.json")))
assert index.is_file() and manifest_path.is_file()
manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
assert manifest["files"] and all(
    Path(str(web.joinpath(name))).is_file() for name in manifest["files"]
)

config = load_config(project / "paf.toml")
assert config.project is not None
assert config.project.root == project.resolve()
assert config.project.config_path == (project / "paf.toml").resolve()
assert config.settings.state_dir == external_state.resolve()
asyncio.run(StateStore(config).load_or_create())
assert (external_state / "state.sqlite3").is_file()
assert not (project / ".paf").exists()
print(origin)
"""


def clean_environment() -> dict[str, str]:
    environment = dict(os.environ)
    for name in (
        "PYTHONHOME",
        "PYTHONPATH",
        "VIRTUAL_ENV",
        "UV_PROJECT",
        "UV_PROJECT_ENVIRONMENT",
        "UV_WORKING_DIR",
    ):
        environment.pop(name, None)
    environment["PYTHONNOUSERSITE"] = "1"
    environment["PYTHONUTF8"] = "1"
    environment["NO_COLOR"] = "1"
    environment["COLUMNS"] = "240"
    return environment


def run(
    command: list[str | Path],
    *,
    cwd: Path,
    environment: dict[str, str],
    timeout: float = 300,
) -> subprocess.CompletedProcess[str]:
    display = list(map(str, command))
    if "-c" in display:
        code = display.index("-c") + 1
        if code < len(display):
            display[code] = "<installed-resource-probe>"
    printable = " ".join(display)
    print(f"+ ({cwd}) {printable}", flush=True)
    completed = subprocess.run(
        [str(item) for item in command],
        cwd=cwd,
        env=environment,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
    )
    if completed.returncode:
        raise RuntimeError(
            f"command failed with exit code {completed.returncode}: {printable}\n{completed.stdout}"
        )
    return completed


def write_project(project: Path, external_state: Path) -> tuple[Path, ...]:
    sources = project / "sources"
    nested = sources / "nested"
    target = project / "lean"
    nested.mkdir(parents=True)
    target.mkdir()
    markdown = sources / "01-markdown.md"
    latex = nested / "02-latex.tex"
    text = nested / "03-plain.txt"
    markdown.write_text(
        "# Markdown notes\n\n## 1. First result\n\nA statement.\n", encoding="utf-8"
    )
    latex.write_text(
        "\\title{LaTeX notes}\n\\section{Second result}\nA statement.\n",
        encoding="utf-8",
    )
    text.write_text("A plain-text mathematical statement.\n", encoding="utf-8")
    (target / "Smoke.lean").write_text(
        "/-- Installed-server smoke declaration. -/\ntheorem Smoke.ok : True := by trivial\n",
        encoding="utf-8",
    )
    (project / "paf.toml").write_text(
        "\n".join(
            (
                "[swarm]",
                'repo = "."',
                f'state_dir = "{external_state.as_posix()}"',
                'isolation = "shared"',
                "",
                "[sources]",
                'roots = ["sources"]',
                "",
                "[backend]",
                'project = "lean"',
                "",
            )
        ),
        encoding="utf-8",
    )
    return markdown, latex, text, sources


def reserve_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def get(url: str) -> tuple[int, str, bytes]:
    request = urllib.request.Request(url, headers={"Accept": "application/json, text/html"})
    with urllib.request.urlopen(request, timeout=2) as response:
        return response.status, response.headers.get_content_type(), response.read()


def smoke_web(
    paf: Path,
    *,
    project: Path,
    outside: Path,
    external_state: Path,
    environment: dict[str, str],
    log_path: Path,
) -> None:
    port = reserve_port()
    with log_path.open("w", encoding="utf-8") as log:
        process = subprocess.Popen(
            [str(paf), "web", str(project), "--host", "127.0.0.1", "--port", str(port)],
            cwd=outside,
            env=environment,
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            deadline = time.monotonic() + 30
            last_error: Exception | None = None
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    break
                try:
                    status, content_type, html = get(f"http://127.0.0.1:{port}/")
                    if status == 200:
                        break
                except (OSError, urllib.error.URLError) as error:
                    last_error = error
                    time.sleep(0.1)
            else:
                raise RuntimeError(f"web server did not start: {last_error}")
            if process.poll() is not None:
                raise RuntimeError("web server exited before smoke tests")

            assert content_type == "text/html" and b'<div id="root">' in html
            script_match = re.search(rb'<script[^>]+src="(?P<src>/assets/[^"]+\.js)"', html)
            assert script_match is not None, "HTML does not reference a bundled JavaScript asset"
            asset_status, asset_type, asset = get(
                f"http://127.0.0.1:{port}{script_match.group('src').decode()}"
            )
            assert asset_status == 200 and "javascript" in asset_type and asset

            runs_status, runs_type, runs_body = get(f"http://127.0.0.1:{port}/api/runs")
            runs = json.loads(runs_body)
            assert runs_status == 200 and runs_type == "application/json" and runs["runs"]
            snapshot_status, _, snapshot_body = get(f"http://127.0.0.1:{port}/api/snapshots")
            snapshot = json.loads(snapshot_body)
            assert snapshot_status == 200 and snapshot["project_root"] == str(project.resolve())
            assert Path(snapshot["source"]) == external_state / "state.json"
            system_status, _, system_body = get(f"http://127.0.0.1:{port}/api/system")
            assert system_status == 200 and "memory_total_bytes" in json.loads(system_body)
            source_status, _, source_body = get(
                f"http://127.0.0.1:{port}/api/source?path=sources/01-markdown.md"
            )
            assert source_status == 200 and "Markdown notes" in json.loads(source_body)["content"]
            target_status, _, target_body = get(
                f"http://127.0.0.1:{port}/api/target?path=Smoke.lean"
            )
            assert target_status == 200 and "Smoke.ok" in json.loads(target_body)["content"]
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
    print(f"web HTML, asset, runs, snapshot, system, source, and target endpoints passed on {port}")


def verify_install(
    archive: Path,
    *,
    scratch: Path,
    checkout: Path,
    uv: Path,
    python: Path,
    environment: dict[str, str],
) -> None:
    label = "wheel" if archive.suffix == ".whl" else "sdist"
    root = scratch / label
    virtualenv = root / "venv"
    outside = root / "unrelated-working-directory"
    project = root / "absolute-project"
    external_state = root / "external-state"
    outside.mkdir(parents=True)
    project.mkdir()
    targets = write_project(project, external_state)

    run([uv, "venv", "--python", python, virtualenv], cwd=outside, environment=environment)
    installed_python = virtualenv / "bin" / "python"
    installed_paf = virtualenv / "bin" / "paf"
    run(
        [uv, "pip", "install", "--python", installed_python, "--strict", archive],
        cwd=outside,
        environment=environment,
        timeout=600,
    )
    version = run([installed_paf, "--version"], cwd=outside, environment=environment)
    assert version.stdout.strip() == "paf 0.7.0", version.stdout

    expected_formats = ("markdown", "latex", "text", None)
    for target, expected_format in zip(targets, expected_formats, strict=True):
        plan = run([installed_paf, "plan", target], cwd=outside, environment=environment)
        assert str(project.resolve()) in plan.stdout
        if expected_format is not None:
            assert expected_format in plan.stdout.casefold(), plan.stdout
        else:
            for source_format in ("markdown", "latex", "text"):
                assert source_format in plan.stdout.casefold(), plan.stdout

    # With no target, a nested cwd must discover the ancestor project-local paf.toml.
    ancestor = run(
        [installed_paf, "plan"],
        cwd=project / "sources" / "nested",
        environment=environment,
    )
    assert str(external_state.resolve()) in ancestor.stdout
    for source_format in ("markdown", "latex", "text"):
        assert source_format in ancestor.stdout.casefold(), ancestor.stdout

    probe = run(
        [
            installed_python,
            "-I",
            "-c",
            RESOURCE_PROBE,
            project,
            external_state,
            checkout,
            virtualenv,
        ],
        cwd=outside,
        environment=environment,
    )
    assert str(virtualenv.resolve()) in probe.stdout
    smoke_web(
        installed_paf,
        project=project,
        outside=outside,
        external_state=external_state,
        environment=environment,
        log_path=root / "web.log",
    )
    print(f"{archive.name}: clean {label} installation passed", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--python",
        type=Path,
        default=Path(sys.executable),
        help=(
            "Python interpreter used for clean virtual environments (default: current interpreter)"
        ),
    )
    parser.add_argument(
        "--keep-temp",
        action="store_true",
        help="retain the temporary build and virtual environments for debugging",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    checkout = args.root.resolve()
    python = args.python.resolve()
    uv_name = shutil.which("uv")
    if uv_name is None:
        raise SystemExit("error: uv is required")
    uv = Path(uv_name).resolve()
    environment = clean_environment()
    if args.keep_temp:
        scratch = Path(tempfile.mkdtemp(prefix="paf-installed-check-"))
        print(f"retaining temporary directory: {scratch}")
        context = None
    else:
        context = tempfile.TemporaryDirectory(prefix="paf-installed-check-")
        scratch = Path(context.name)
    try:
        dist = scratch / "dist"
        run(
            [uv, "build", "--out-dir", dist, "--clear", checkout],
            cwd=scratch,
            environment=environment,
            timeout=600,
        )
        wheels = sorted(dist.glob("*.whl"))
        sdists = sorted(dist.glob("*.tar.gz"))
        if len(wheels) != 1 or len(sdists) != 1:
            raise RuntimeError(f"expected one wheel and one sdist, found: {sorted(dist.iterdir())}")
        for archive in (*wheels, *sdists):
            problems = check_archive(archive)
            if problems:
                raise RuntimeError(
                    f"{archive} has invalid contents:\n"
                    + "\n".join(f"  - {problem}" for problem in problems)
                )
            print(f"{archive.name}: archive contents passed", flush=True)
        for archive in (*wheels, *sdists):
            verify_install(
                archive,
                scratch=scratch,
                checkout=checkout,
                uv=uv,
                python=python,
                environment=environment,
            )
    finally:
        if context is not None:
            context.cleanup()
    print("wheel and sdist outside-directory installation checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
