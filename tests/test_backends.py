from __future__ import annotations

import asyncio
import subprocess
from pathlib import Path
from typing import Any

import pytest
from rich.console import Console

from paf.backends import LeanBackend
from paf.cli import print_plan
from paf.codex import CodexExecutor, ValidationResult
from paf.config import load_config
from paf.models import Stage
from paf.scheduler import Orchestrator, scaffold_directories
from paf.state import StateStore
from tests.support import write_project


def _mixed_project(tmp_path: Path, *, mapping: str = "") -> Path:
    sources = tmp_path / "sources"
    (sources / "nested").mkdir(parents=True)
    (sources / "guide.md").write_text("# Guide\n\n## Opening\nMarkdown.\n", encoding="utf-8")
    (sources / "nested" / "notes.tex").write_text(
        "\\section{TeX unit}\nMathematics.\n", encoding="utf-8"
    )
    (sources / "appendix.txt").write_text("A plain text unit.\n", encoding="utf-8")
    path = tmp_path / "paf.toml"
    path.write_text(
        f"""
[swarm]
repo = "."
isolation = "shared"

[sources]
roots = ["sources"]

[sources.dependencies]
"sources/nested/notes.tex" = ["sources/guide.md"]

[[sources.rules]]
glob = "**/*.tex"
unit = "section"

[backend]
kind = "lean"
project = "lean-project"
root = "generated/targets"
module = "Flat"
path = "Unit{{unit_ordinal_padded}}_{{source_stem}}"
unit_module = "{{module}}.{{document_module}}.U{{unit_ordinal}}"
build_command = "true"
scope = ["{{root}}/{{path}}.lean"]
{mapping}
""",
        encoding="utf-8",
    )
    return path


def test_backend_templates_map_nested_mixed_sources_to_flat_targets(tmp_path: Path) -> None:
    config = load_config(_mixed_project(tmp_path))

    assert [document.format for document in config.documents] == ["text", "markdown", "latex"]
    assert [unit.chapter_path for unit in config.work_units] == [
        "Unit01_appendix",
        "Unit01_guide",
        "Unit01_notes",
    ]
    tex = config.work_unit("sources/nested/notes/unit-01")
    assert tex.document.depends_on == ("sources/guide",)
    assert tex.chapter_module == "Flat.Sources.Nested.Notes.U1"
    assert tex.scope == ("generated/targets/Unit01_notes.lean",)
    assert tex.build_command == "true"


def test_default_backend_uses_nested_paths_and_dotted_modules(tmp_path: Path) -> None:
    (tmp_path / "books").mkdir()
    (tmp_path / "books" / "crystalline.tex").write_text(
        "\\section{Introduction}\nText.\n", encoding="utf-8"
    )
    config_path = tmp_path / "paf.toml"
    config_path.write_text(
        """
[swarm]
repo = "."
isolation = "shared"

[sources]
roots = ["books"]

[[sources.rules]]
glob = "**/*.tex"
unit = "section"

[backend]
kind = "lean"
""",
        encoding="utf-8",
    )

    unit = load_config(config_path).work_unit("books/crystalline/unit-01")
    assert unit.chapter_path == "Books/Crystalline/Unit01"
    assert unit.chapter_module == "Formalization.Books.Crystalline.Unit01"
    assert unit.scope == (
        "lean/Formalization/Books/Crystalline/Unit01.lean",
        "lean/Formalization/Books/Crystalline/Unit01/**/*.lean",
    )


def test_backend_configures_beam(tmp_path: Path) -> None:
    config_path = _mixed_project(tmp_path)
    config_path.write_text(
        config_path.read_text(encoding="utf-8").replace(
            'project = "lean-project"',
            'project = "lean-project"\nbeam_command = "/opt/beam/bin/lean-beam"\n'
            "beam_startup_timeout_seconds = 75",
        ),
        encoding="utf-8",
    )

    backend = load_config(config_path).backend

    assert isinstance(backend, LeanBackend)
    assert backend.beam_command == "/opt/beam/bin/lean-beam"
    assert backend.beam_startup_timeout_seconds == 75


@pytest.mark.parametrize(
    "setting",
    ['tool_driver = "mcp"', "mcp_enabled = true", "mcp_tool_timeout_seconds = 300"],
)
def test_backend_rejects_removed_mcp_settings(tmp_path: Path, setting: str) -> None:
    config_path = _mixed_project(tmp_path)
    config_path.write_text(
        config_path.read_text(encoding="utf-8").replace(
            'project = "lean-project"', f'project = "lean-project"\n{setting}'
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="unknown backend keys"):
        load_config(config_path)


def test_backend_bootstraps_mathlib_project_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[tuple[list[str], Path]] = []

    def run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append((command, kwargs["cwd"]))
        stdout = "Lake version test (Lean version 4.33.0-rc2)" if "--version" in command else ""
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    monkeypatch.setattr("paf.backends.shutil.which", lambda *_args, **_kwargs: "/bin/lake")
    monkeypatch.setattr("paf.backends.subprocess.run", run)
    backend = LeanBackend()

    assert backend.prepare_project(tmp_path, timeout_seconds=30)
    project = tmp_path / "lean"
    assert (project / "lean-toolchain").read_text(encoding="utf-8") == (
        "leanprover/lean4:v4.33.0-rc2\n"
    )
    lakefile = (project / "lakefile.toml").read_text(encoding="utf-8")
    assert 'rev = "v4.33.0-rc2"' in lakefile
    assert 'name = "mathlib"' in lakefile
    assert 'name = "Formalization"' in lakefile
    assert calls == [
        (["/bin/lake", "--version"], project),
        (["/bin/lake", "update"], project),
    ]

    assert not backend.prepare_project(tmp_path, timeout_seconds=30)
    assert len(calls) == 2


def test_backend_retries_interrupted_bootstrap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = tmp_path / "lean"
    project.mkdir()
    (project / "lean-toolchain").write_text("leanprover/lean4:v4.33.0-rc2\n", encoding="utf-8")
    (project / "lakefile.lean").write_text("package Paf\n", encoding="utf-8")
    (project / ".paf-bootstrap-pending").touch()
    commands: list[list[str]] = []

    def run(command: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        commands.append(command)
        stdout = "Lake version test (Lean version 4.33.0-rc2)" if "--version" in command else ""
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    monkeypatch.setattr("paf.backends.shutil.which", lambda *_args, **_kwargs: "/bin/lake")
    monkeypatch.setattr("paf.backends.subprocess.run", run)

    assert LeanBackend().prepare_project(tmp_path, timeout_seconds=30)
    assert commands[-1] == ["/bin/lake", "update"]
    assert not (project / ".paf-bootstrap-pending").exists()


def test_explicit_backend_mapping_overrides_arbitrary_layout(tmp_path: Path) -> None:
    config = load_config(
        _mixed_project(
            tmp_path,
            mapping="""
[[backend.mappings]]
work_unit = "sources/nested/notes/unit-01"
root = "formal/elsewhere"
path = "Hand/Chosen"
unit_module = "Unrelated.Namespace.TheoremFile"
scope = ["formal/custom/theorem.lean", "formal/helpers/**/*.lean"]
""",
        )
    )

    unit = config.work_unit("sources/nested/notes/unit-01")
    assert unit.lean_root == Path("formal/elsewhere")
    assert unit.chapter_path == "Hand/Chosen"
    assert unit.chapter_module == "Unrelated.Namespace.TheoremFile"
    assert unit.scope == ("formal/custom/theorem.lean", "formal/helpers/**/*.lean")


def test_backend_manifest_maps_work_unit(tmp_path: Path) -> None:
    manifest = tmp_path / "targets.json"
    manifest.write_text(
        """{"sources/appendix/unit-01": {
          "path": "Manifest/Appendix", "unit_module": "Chosen.Appendix",
          "scope": ["chosen/Appendix.lean"]}}""",
        encoding="utf-8",
    )
    config_path = _mixed_project(tmp_path)
    config_path.write_text(
        config_path.read_text(encoding="utf-8").replace(
            'project = "lean-project"', 'project = "lean-project"\nmanifest = "targets.json"'
        ),
        encoding="utf-8",
    )

    unit = load_config(config_path).work_unit("sources/appendix/unit-01")
    assert unit.chapter_path == "Manifest/Appendix"
    assert unit.chapter_module == "Chosen.Appendix"
    assert unit.scope == ("chosen/Appendix.lean",)


def test_plan_lists_documents_units_dependencies_spans_and_scopes(tmp_path: Path) -> None:
    config = load_config(_mixed_project(tmp_path))
    console = Console(record=True, width=240)

    print_plan(config, console)
    output = console.export_text()

    assert "Documents (critical-path priority order)" in output
    assert "sources/guide.md" in output
    assert "sources/nested/notes.tex" in output
    assert "sources/appendix.txt" in output
    assert "sources/nested/notes/unit-01" in output
    assert "sources/nested/notes.tex:1-2" in output
    assert "sources/guide" in output
    assert "generated/targets/Unit01_notes.lean" in output


def test_mixed_scaffold_and_orchestration_setup_use_backend(tmp_path: Path) -> None:
    config = load_config(_mixed_project(tmp_path))

    created = scaffold_directories(config, config.work_units)
    assert set(created) == {
        "generated/targets/Unit01_appendix",
        "generated/targets/Unit01_guide",
        "generated/targets/Unit01_notes",
    }

    async def setup() -> None:
        state = StateStore(config)
        await state.load_or_create()
        orchestrator = Orchestrator(config, state)
        assert len(orchestrator.work_units) == 3
        assert {unit.document.format for unit in config.work_units} == {
            "markdown",
            "latex",
            "text",
        }
        command = CodexExecutor(config, state).command(Stage.PROVE)
        assert not any("mcp_servers.paf_lean" in argument for argument in command)

    asyncio.run(setup())


def test_state_payloads_include_source_path_and_span(tmp_path: Path) -> None:
    config = load_config(_mixed_project(tmp_path))
    unit = config.work_unit("sources/nested/notes/unit-01")

    async def snapshot() -> dict[str, Any]:
        state = StateStore(config)
        await state.load_or_create()
        await state.start_run(unit.id, Stage.FORMALIZE)
        return state.snapshot()

    payload = asyncio.run(snapshot())
    task = payload["tasks"][f"{unit.id}:formalize"]
    run = task["runs"][0]
    assert (task["source"], task["source_start_line"], task["source_end_line"]) == (
        "sources/nested/notes.tex",
        1,
        2,
    )
    assert (run["source"], run["source_start_line"], run["source_end_line"]) == (
        "sources/nested/notes.tex",
        1,
        2,
    )


def test_routed_diagnostics_identify_informal_source_span(tmp_path: Path) -> None:
    config = load_config(_mixed_project(tmp_path))
    unit = config.work_unit("sources/nested/notes/unit-01")
    orchestrator = Orchestrator(config, StateStore(config))

    diagnostics = orchestrator._build_feedback(
        {
            unit.id: ValidationResult(
                False,
                1,
                "error: generated/targets/Unit01_notes.lean:1:1: synthetic failure",
            )
        }
    )

    assert "Informal source: sources/nested/notes.tex:1-2" in diagnostics.actionable[unit.id]


def test_legacy_books_keep_state_ids_and_per_book_targets(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path))

    assert [unit.id for unit in config.work_units] == ["book/chapter-01", "book/chapter-02"]
    assert config.work_units[0].scope == (
        "lean/Book/Chapter01.lean",
        "lean/Book/Chapter01/**/*.lean",
    )
