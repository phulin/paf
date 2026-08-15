from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest

from paf.resolver import SourceResolver


def _write(path: Path, text: str = "text\n") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def test_recurses_sorts_mixed_formats_and_deduplicates_overlapping_roots(
    tmp_path: Path,
) -> None:
    _write(tmp_path / "notes" / "z.txt")
    _write(tmp_path / "notes" / "deep" / "b.tex", "\\section{B}\n")
    _write(tmp_path / "notes" / "deep" / "a.md", "## A\n")
    _write(tmp_path / "notes" / "ignored.pdf")

    resolver = SourceResolver(tmp_path)
    resolved = resolver.resolve_all(("notes", "notes/deep/a.md", "notes/deep"))

    assert [document.path.as_posix() for document in resolved.documents] == [
        "notes/deep/a.md",
        "notes/deep/b.tex",
        "notes/z.txt",
    ]
    assert [document.format for document in resolved.documents] == [
        "markdown",
        "latex",
        "text",
    ]
    assert [unit.title for unit in resolved.work_units] == ["A", "B", "Z"]
    assert resolver.resolve("notes/deep/a.md")[0].path == Path("notes/deep/a.md")


def test_ordered_patterns_can_exclude_then_restore_a_path(tmp_path: Path) -> None:
    _write(tmp_path / "notes" / "keep.md", "## Keep\n")
    _write(tmp_path / "notes" / "drafts" / "drop.md", "## Drop\n")
    _write(tmp_path / "notes" / "drafts" / "keep.md", "## Restored\n")

    paths = SourceResolver(
        tmp_path,
        include=("**/*.md",),
        exclude=("**/drafts/**", "!**/drafts/keep.md"),
    ).discover_paths(("notes",))

    assert [path.as_posix() for path in paths] == [
        "notes/drafts/keep.md",
        "notes/keep.md",
    ]


def test_ignores_metadata_hidden_build_and_symlinked_directories(tmp_path: Path) -> None:
    _write(tmp_path / "visible" / "source.md", "## Visible\n")
    for name in (".git", ".paf", ".hidden", "build", "dist", "target", "_build"):
        _write(tmp_path / name / "ignored.md", "## Ignored\n")
    external = tmp_path / "external"
    _write(external / "linked.md", "## Linked\n")
    link = tmp_path / "visible" / "linked-directory"
    try:
        os.symlink(external, link, target_is_directory=True)
    except OSError:
        pytest.skip("directory symlinks are unavailable")

    paths = SourceResolver(tmp_path).discover_paths((tmp_path,))

    assert paths == (Path("external/linked.md"), Path("visible/source.md"))


def test_document_dependencies_accept_paths_and_ids(tmp_path: Path) -> None:
    _write(tmp_path / "a.md", "## A\n")
    _write(tmp_path / "nested" / "b.tex", "\\section{B}\n")

    resolved = SourceResolver(
        tmp_path,
        dependencies={"nested/b.tex": ["a.md", "a"]},
    ).resolve((tmp_path,))

    assert resolved[1].id == "nested/b"
    assert resolved[1].depends_on == ("a",)


def test_manifest_order_precedes_remaining_sorted_documents(tmp_path: Path) -> None:
    for name in ("a.md", "b.md", "c.md"):
        _write(tmp_path / name, f"## {name}\n")

    documents = SourceResolver(tmp_path, manifest=["c.md", "a.md"]).resolve((tmp_path,))

    assert [document.path.as_posix() for document in documents] == ["c.md", "a.md", "b.md"]


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"manifest": ["missing.md"]}, "undiscovered"),
        ({"manifest": ["a.md", "a.md"]}, "duplicate"),
        ({"dependencies": {"missing": []}}, "unknown document"),
        ({"dependencies": {"a": ["missing"]}}, "unknown document"),
        ({"dependencies": {"a": ["a"]}}, "depend on itself"),
    ],
)
def test_reports_manifest_and_dependency_errors(
    tmp_path: Path, kwargs: dict[str, Any], message: str
) -> None:
    _write(tmp_path / "a.md", "## A\n")

    with pytest.raises(ValueError, match=message):
        SourceResolver(tmp_path, **kwargs).resolve((tmp_path,))


def test_reports_document_dependency_cycles(tmp_path: Path) -> None:
    _write(tmp_path / "a.md", "## A\n")
    _write(tmp_path / "b.md", "## B\n")

    with pytest.raises(ValueError, match=r"dependency cycle: a -> b -> a"):
        SourceResolver(tmp_path, dependencies={"a": ["b"], "b": ["a"]}).resolve(tmp_path)


def test_path_rules_are_ordered_and_last_match_controls_adapter(tmp_path: Path) -> None:
    _write(tmp_path / "lectures" / "one.tex", "\\chapter{No}\\section{Yes}\n")
    resolver = SourceResolver(
        tmp_path,
        rules=(
            {"glob": "**/*.tex", "unit": "chapter"},
            {"glob": "lectures/**/*.tex", "format": "latex", "unit": "section"},
        ),
    )

    assert [unit.title for unit in resolver.resolve_all(("lectures",)).work_units] == ["Yes"]


def test_rejects_empty_missing_and_outside_roots(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="at least one"):
        SourceResolver(tmp_path).discover_paths(())
    with pytest.raises(ValueError, match="does not exist"):
        SourceResolver(tmp_path).discover_paths(("missing",))
    outside = _write(tmp_path.parent / f"{tmp_path.name}-outside.md", "## Outside\n")
    with pytest.raises(ValueError, match="inside source repository"):
        SourceResolver(tmp_path).discover_paths((outside,))
