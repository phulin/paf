#!/usr/bin/env python3
"""Verify metadata and runtime resources in PAF wheel and sdist archives."""

from __future__ import annotations

import argparse
import sys
import tarfile
import zipfile
from pathlib import Path

PROMPTS = {
    "common.md",
    "discover.md",
    "formalize.md",
    "proof_review.md",
    "prove.md",
    "review.md",
    "upstream_repair.md",
}


def archive_names(path: Path) -> set[str]:
    if path.suffix == ".whl":
        with zipfile.ZipFile(path) as archive:
            return set(archive.namelist())
    if path.name.endswith(".tar.gz"):
        with tarfile.open(path, "r:gz") as archive:
            return {member.name for member in archive.getmembers() if member.isfile()}
    raise ValueError(f"unsupported distribution archive: {path}")


def archive_bytes(path: Path, suffix: str) -> bytes:
    """Read the unique archive member whose normalized name ends with *suffix*."""

    names = sorted(name for name in archive_names(path) if name.endswith(suffix))
    if len(names) != 1:
        raise ValueError(f"expected one archive member ending in {suffix!r}, found {len(names)}")
    name = names[0]
    if path.suffix == ".whl":
        with zipfile.ZipFile(path) as archive:
            return archive.read(name)
    with tarfile.open(path, "r:gz") as archive:
        member = archive.extractfile(name)
        if member is None:
            raise ValueError(f"cannot read archive member: {name}")
        return member.read()


def package_suffix(path: Path) -> str:
    return "paf/" if path.suffix == ".whl" else "src/paf/"


def check_archive(path: Path) -> list[str]:
    names = archive_names(path)
    suffix = package_suffix(path)

    def contains(relative: str) -> bool:
        return any(name.endswith(suffix + relative) for name in names)

    problems = [
        f"missing prompt {name}" for name in sorted(PROMPTS) if not contains(f"prompts/{name}")
    ]
    for relative in ("__init__.py", "cli.py", "config.py", "web.py"):
        if not contains(relative):
            problems.append(f"missing paf/{relative}")
    for relative in ("web_dist/index.html", "web_dist/bundle-manifest.json"):
        if not contains(relative):
            problems.append(f"missing {relative}")
    if not any(f"{suffix}web_dist/assets/" in name and name.rsplit("/", 1)[-1] for name in names):
        problems.append("missing web_dist/assets")
    if any("node_modules/" in name for name in names):
        problems.append("archive unexpectedly contains node_modules")

    try:
        if path.suffix == ".whl":
            entry_points = archive_bytes(path, ".dist-info/entry_points.txt").decode()
            if "paf = paf.cli:main" not in entry_points:
                problems.append("wheel console entry point is not paf = paf.cli:main")
        else:
            pyproject = archive_bytes(path, "/pyproject.toml").decode()
            if 'paf = "paf.cli:main"' not in pyproject:
                problems.append("sdist console entry point is not paf = paf.cli:main")
    except (KeyError, UnicodeDecodeError, ValueError) as error:
        problems.append(f"cannot verify console entry point: {error}")
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archives", type=Path, nargs="+")
    args = parser.parse_args()
    failed = False
    for archive in args.archives:
        problems = check_archive(archive)
        if problems:
            failed = True
            print(f"{archive}:", file=sys.stderr)
            for problem in problems:
                print(f"  - {problem}", file=sys.stderr)
        else:
            print(f"{archive}: metadata, packaged prompts, and frontend verified")
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
