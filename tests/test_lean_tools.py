from __future__ import annotations

from pathlib import Path

from paf.lean_tools import prepare_lean_dependencies, search_lean_sources


def test_search_lean_sources_covers_project_and_lake_packages(tmp_path: Path) -> None:
    (tmp_path / "Main.lean").write_text("theorem sought : True := by trivial\n", encoding="utf-8")
    dependency = tmp_path / ".lake" / "packages" / "dep"
    dependency.mkdir(parents=True)
    (dependency / "Dep.lean").write_text(
        "lemma sought_dep : True := by trivial\n", encoding="utf-8"
    )

    matches = search_lean_sources("sought", root=tmp_path)

    assert [(item["source"], item["path"]) for item in matches] == [
        ("project", "Main.lean"),
        ("dependency", ".lake/packages/dep/Dep.lean"),
    ]


def test_prepare_lean_dependencies_follows_beam_save_deps(tmp_path: Path) -> None:
    beam = tmp_path / "lean-beam"
    state = tmp_path / "state"
    beam.write_text(
        f"""#!/usr/bin/env python3
import json
import pathlib
import sys

state = pathlib.Path({str(state)!r})
operation, file = sys.argv[-2:]
if operation == "sync" and file == "Main.lean" and not state.exists():
    print(json.dumps({{"error": {{"data": {{"saveDeps": ["Dep.lean"]}}}}}}))
    raise SystemExit(1)
if operation == "save":
    state.write_text(file)
print(json.dumps({{"result": {{"file": file}}}}))
""",
        encoding="utf-8",
    )
    beam.chmod(0o755)

    result = prepare_lean_dependencies(
        ["Main.lean"], root=tmp_path, beam_command=str(beam), max_rounds=3
    )

    assert result["ok"] is True
    assert result["prepared"] == ["Main.lean"]
    assert state.read_text(encoding="utf-8") == "Dep.lean"


def test_prepare_lean_dependencies_refreshes_target_from_recovery_plan(tmp_path: Path) -> None:
    beam = tmp_path / "lean-beam"
    state = tmp_path / "state"
    script = """#!/usr/bin/env python3
import json
import pathlib
import sys

state = pathlib.Path(__STATE__)
operation, file = sys.argv[-2:]
if operation == "sync" and not state.exists():
    print(json.dumps({"error": {"code": "syncBarrierIncomplete", "data": {
        "recoveryPlan": [f'lean-beam refresh "{file}"', "lake build"],
        "saveDeps": [],
        "staleDirectDeps": [],
        "targetPath": file,
    }}}))
    raise SystemExit(1)
if operation == "refresh":
    state.write_text(file)
print(json.dumps({"result": {"file": file}}))
"""
    beam.write_text(
        script.replace("__STATE__", repr(str(state))),
        encoding="utf-8",
    )
    beam.chmod(0o755)

    result = prepare_lean_dependencies(
        ["Main.lean"], root=tmp_path, beam_command=str(beam), max_rounds=3
    )

    assert result["ok"] is True
    assert result["prepared"] == ["Main.lean"]
    assert state.read_text(encoding="utf-8") == "Main.lean"
