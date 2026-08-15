import asyncio
import json
import shutil
from pathlib import Path

import pytest

import paf.cli as cli_module
from paf.cli import main
from paf.config import infer_config, load_config
from paf.models import PipelineConfig, Stage
from paf.project import ProjectResolver
from paf.state import StateStore
from paf.state_db import read_full_snapshot
from tests.support import write_project


def test_explicit_project_and_paf_toml_resolve_all_project_paths(tmp_path: Path) -> None:
    config_path = write_project(tmp_path)

    directory = ProjectResolver(tmp_path.parent).resolve(project=tmp_path)
    file = ProjectResolver(tmp_path.parent).resolve(project=config_path)
    config = load_config(config_path, project=directory)

    assert directory.root == tmp_path
    assert directory.config_path == config_path
    assert file.root == tmp_path
    assert config.project is not None
    assert config.project.root == tmp_path
    assert config.project.source_paths == (tmp_path / "books" / "book.md",)
    assert config.project.target_dir == tmp_path / "lean"
    assert config.project.state_dir == tmp_path / ".paf"


def test_target_prefers_ancestor_config_then_git_and_cwd_fallback(tmp_path: Path) -> None:
    project = tmp_path / "configured"
    nested = project / "notes" / "deep"
    nested.mkdir(parents=True)
    (project / "paf.toml").write_text("", encoding="utf-8")
    target = nested / "source.md"
    target.write_text("# Source\n", encoding="utf-8")

    assert ProjectResolver(tmp_path).resolve(targets=(target,)).root == project

    git_project = tmp_path / "git-project"
    (git_project / ".git").mkdir(parents=True)
    git_target = git_project / "sources" / "source.md"
    git_target.parent.mkdir()
    git_target.write_text("# Source\n", encoding="utf-8")
    assert ProjectResolver(tmp_path).resolve(targets=(git_target,)).root == git_project

    unrelated = tmp_path / "unrelated"
    unrelated.mkdir()
    fallback = ProjectResolver(unrelated).resolve()
    assert fallback.root == unrelated
    assert fallback.state_dir == unrelated / ".paf"


def test_ancestor_config_is_discovered_from_nested_cwd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = write_project(tmp_path)
    nested = tmp_path / "one" / "two"
    nested.mkdir(parents=True)
    monkeypatch.chdir(nested)

    assert main(["plan"]) == 0
    assert ProjectResolver().resolve().config_path == path


def test_explicit_project_works_outside_checkout_for_source_and_control_commands(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    write_project(project)
    outside = tmp_path / "outside"
    outside.mkdir()
    monkeypatch.chdir(outside)
    asyncio.run(StateStore(load_config(project / "paf.toml")).load_or_create())

    assert main(["plan", "--project", str(project)]) == 0
    assert str(project) in capsys.readouterr().out
    assert main(["status", "--project", str(project), "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["project_root"] == str(project)
    assert main(["source-issues", "--project", str(project), "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["issues"] == []
    assert main(["scaffold", "--project", str(project)]) == 0
    assert (project / "lean" / "Book" / "Chapter01").is_dir()

    observed: list[Path] = []

    def agent_command(_args: object, config: PipelineConfig) -> int:
        assert config.project is not None
        observed.append(config.project.root)
        return 0

    monkeypatch.setattr(cli_module, "_agent_command", agent_command)
    for command in ("status", "snapshot", "pause", "resume", "unblock", "stop", "wait", "rpc"):
        assert main(["agent", command, "--project", str(project)]) == 0
    assert observed == [project] * 8


def test_absolute_target_works_outside_checkout_and_keeps_legacy_target_semantics(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = tmp_path / "project"
    (project / ".git").mkdir(parents=True)
    source = project / "books" / "07-topic.md"
    source.parent.mkdir()
    source.write_text("# Topic\n\n## 1. Opening\n", encoding="utf-8")
    outside = tmp_path / "outside"
    outside.mkdir()
    monkeypatch.chdir(outside)

    config = infer_config(source, project=ProjectResolver().resolve(targets=(source,)))

    assert config.settings.repo == project
    assert config.project is not None
    assert config.project.source_paths == (source,)
    assert config.settings.state_dir == project / ".paf" / "book07"
    assert main(["plan", str(source)]) == 0


def test_external_state_directory_and_project_metadata_are_durable(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    config_path = write_project(project, chapters="chapters = [1]")
    external = tmp_path / "external-state"
    config_path.write_text(
        config_path.read_text(encoding="utf-8").replace(
            'repo = "."', f'repo = "."\nstate_dir = "{external}"'
        ),
        encoding="utf-8",
    )
    config = load_config(config_path)
    state = StateStore(config)

    async def populate() -> str:
        await state.load_or_create()
        run = await state.start_run(config.work_units[0].id, Stage.FORMALIZE)
        return run.id

    run_id = asyncio.run(populate())
    snapshot = read_full_snapshot(external)

    assert snapshot is not None
    assert config.project is not None and config.project.state_dir == external
    assert snapshot["project_root"] == str(project)
    run = snapshot["tasks"][f"{config.work_units[0].id}:formalize"]["runs"][0]
    assert run["id"] == run_id
    assert run["project_root"] == str(project)


def test_project_local_state_rebinds_checkpoint_after_project_is_moved(tmp_path: Path) -> None:
    original = tmp_path / "original"
    original.mkdir()
    original_config = load_config(write_project(original, chapters="chapters = [1]"))
    original_state = StateStore(original_config)
    asyncio.run(original_state.load_or_create())

    moved = tmp_path / "moved"
    shutil.copytree(original, moved)
    moved_config = load_config(moved / "paf.toml")
    moved_state = StateStore(moved_config)
    asyncio.run(moved_state.load_or_create())
    snapshot = read_full_snapshot(moved / ".paf")

    assert snapshot is not None
    assert snapshot["project_root"] == str(moved)
    assert snapshot["config"] == str(moved / "paf.toml")
    assert moved_config.work_units[0].id == original_config.work_units[0].id
