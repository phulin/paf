#!/usr/bin/env python3
"""Build or validate the locally generated PAF web bundle.

The check deliberately uses content hashes, not mtimes. Git does not preserve
mtimes, while a content manifest gives the same answer in every checkout and
also detects input files whose timestamp happens to be older than the bundle.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

from xxhash import xxh3_64

MANIFEST_NAME = "bundle-manifest.json"
MANIFEST_VERSION = 2
HASHED_ASSET = re.compile(r"^assets/.+-[A-Za-z0-9_-]{8,}\.[^.]+$")
WEB_CONFIG_FILES = (
    "index.html",
    "package-lock.json",
    "package.json",
    "tsconfig.app.json",
    "tsconfig.json",
    "tsconfig.node.json",
    "vite.config.ts",
)


class BundleError(RuntimeError):
    """A generated bundle is absent, corrupt, or stale."""


def content_digest(path: Path) -> str:
    digest = xxh3_64()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def relative_files(directory: Path) -> list[Path]:
    if not directory.is_dir():
        return []
    return sorted(
        (path.relative_to(directory) for path in directory.rglob("*") if path.is_file()),
        key=lambda path: path.as_posix(),
    )


def input_paths(root: Path) -> list[Path]:
    web = root / "web"
    paths = [web / name for name in WEB_CONFIG_FILES]
    paths.extend(web / "src" / path for path in relative_files(web / "src"))
    missing = [path for path in paths if not path.is_file()]
    if missing:
        joined = ", ".join(str(path.relative_to(root)) for path in missing)
        raise BundleError(f"frontend inputs are missing: {joined}")
    return sorted(paths, key=lambda path: path.relative_to(root).as_posix())


def input_hashes(root: Path) -> dict[str, str]:
    return {path.relative_to(root).as_posix(): content_digest(path) for path in input_paths(root)}


def output_hashes(root: Path) -> dict[str, str]:
    output = root / "src" / "paf" / "web_dist"
    return {
        path.as_posix(): content_digest(output / path)
        for path in relative_files(output)
        if path.as_posix() != MANIFEST_NAME
    }


def manifest_data(root: Path) -> dict[str, Any]:
    return {
        "format": MANIFEST_VERSION,
        "inputs": input_hashes(root),
        "files": output_hashes(root),
    }


def write_manifest(root: Path) -> None:
    output = root / "src" / "paf" / "web_dist"
    files = output_hashes(root)
    if "index.html" not in files:
        raise BundleError("frontend build did not produce index.html")
    assets = [name for name in files if name.startswith("assets/")]
    if not assets or any(not HASHED_ASSET.fullmatch(name) for name in assets):
        raise BundleError("frontend build must contain only content-hashed files under assets/")
    manifest = {"format": MANIFEST_VERSION, "inputs": input_hashes(root), "files": files}
    (output / MANIFEST_NAME).write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def build(root: Path) -> None:
    npm = shutil.which("npm")
    if npm is None:
        raise BundleError("npm is required to rebuild the frontend (but not to install PAF)")
    subprocess.run([npm, "run", "build"], cwd=root / "web", check=True)
    write_manifest(root)
    check(root)


def load_manifest(root: Path) -> dict[str, Any]:
    path = root / "src" / "paf" / "web_dist" / MANIFEST_NAME
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise BundleError(f"generated frontend manifest is missing: {path}") from error
    except json.JSONDecodeError as error:
        raise BundleError(f"invalid frontend manifest: {error}") from error
    if not isinstance(value, dict):
        raise BundleError("frontend manifest must be a JSON object")
    return value


def differences(expected: object, actual: object, label: str) -> list[str]:
    if not isinstance(expected, dict) or not isinstance(actual, dict):
        return [f"{label} manifest entry is invalid"]
    messages: list[str] = []
    expected_keys = set(expected)
    actual_keys = set(actual)
    for name in sorted(actual_keys - expected_keys):
        messages.append(f"new {label}: {name}")
    for name in sorted(expected_keys - actual_keys):
        messages.append(f"missing {label}: {name}")
    for name in sorted(expected_keys & actual_keys):
        if expected[name] != actual[name]:
            messages.append(f"changed {label}: {name}")
    return messages


def check(root: Path) -> None:
    manifest = load_manifest(root)
    problems: list[str] = []
    if manifest.get("format") != MANIFEST_VERSION:
        problems.append(
            f"unsupported manifest format: {manifest.get('format')!r} (expected {MANIFEST_VERSION})"
        )
    problems.extend(differences(manifest.get("inputs"), input_hashes(root), "input"))
    current_outputs = output_hashes(root)
    problems.extend(differences(manifest.get("files"), current_outputs, "bundle file"))
    if "index.html" not in current_outputs:
        problems.append("missing bundle file: index.html")
    asset_names = [name for name in current_outputs if name.startswith("assets/")]
    if not asset_names:
        problems.append("bundle has no assets")
    for name in asset_names:
        if not HASHED_ASSET.fullmatch(name):
            problems.append(f"bundle asset is not content-hashed: {name}")
    if problems:
        details = "\n".join(f"  - {problem}" for problem in problems)
        raise BundleError(
            "generated frontend bundle is stale or corrupt:\n"
            f"{details}\n"
            "Run `python scripts/web_bundle.py build` to regenerate src/paf/web_dist/."
        )


def repository_root(script: Path) -> Path:
    return script.resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("build", "check"))
    parser.add_argument(
        "--root",
        type=Path,
        default=repository_root(Path(__file__)),
        help=argparse.SUPPRESS,
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        if args.command == "build":
            build(args.root.resolve())
        else:
            check(args.root.resolve())
    except (BundleError, subprocess.CalledProcessError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    message = (
        "frontend bundle rebuilt and verified"
        if args.command == "build"
        else "frontend bundle is fresh"
    )
    print(message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
