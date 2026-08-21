import asyncio
import json
import shutil
import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

import paf.cli as cli_module
import paf.state as state_module
from paf.cli import main
from paf.config import infer_config, load_config
from paf.models import PipelineConfig, Stage
from paf.project import ProjectResolver
from paf.state import StateStore, TaskStatus, TokenUsage
from paf.state_db import SCHEMA_VERSION, StateDatabase, read_checkpoint, read_full_snapshot
from tests.support import write_project


@pytest.mark.asyncio
async def test_task_and_build_deltas_do_not_rewrite_global_checkpoint(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    await state.load_or_create()
    database = config.settings.state_dir / "state.sqlite3"

    def global_row(key: str) -> tuple[int, bytes] | None:
        with sqlite3.connect(database) as connection:
            row = connection.execute(
                "SELECT revision, payload FROM globals WHERE key=?", (key,)
            ).fetchone()
        return (int(row[0]), bytes(row[1])) if row is not None else None

    checkpoint = global_row("state")
    assert checkpoint is not None
    header = json.loads(checkpoint[1])
    assert not {
        "scheduling",
        "source_dependency_tree",
        "formalize_graph",
        "upstream_requests",
        "proof_blockers",
        "repair_cases",
        "thread_cumulative_usage",
    }.intersection(header)
    assert len(checkpoint[1]) < 10_000
    chapter = config.chapters[0]
    await state.set_task(chapter.id, Stage.FORMALIZE, TaskStatus.RUNNING, "agent running")
    assert global_row("state") == checkpoint

    await state.start_coordinator_build(
        mode="test",
        stage=Stage.FORMALIZE,
        iteration=1,
        maximum_iterations=1,
        total=1,
        target_work_unit_ids=(chapter.id,),
    )
    build = global_row("coordinator_build")
    assert build is not None
    assert len(build[1]) < 10_000
    assert global_row("state") == checkpoint

    state.append_coordinator_build_output("✔ [1/1] Built Book.Chapter01")
    await state.flush()
    assert global_row("coordinator_build") != build
    assert global_row("state") == checkpoint
    await state.record_thread_cumulative_usage(
        "thread-1",
        TokenUsage(input_tokens=100, output_tokens=10, measured=True),
        deferred=False,
    )
    assert global_row("thread_cumulative_usage") is None
    with sqlite3.connect(database) as connection:
        usage_row = connection.execute(
            """
            SELECT payload FROM state_items
            WHERE section='thread_cumulative_usage' AND item_key='thread-1'
            """
        ).fetchone()
    assert usage_row is not None
    usage_payload = json.loads(usage_row[0])
    assert usage_payload["input_tokens"] + usage_payload["output_tokens"] == 110
    assert global_row("state") == checkpoint
    await state.finish_coordinator_build()
    await state.close()


@pytest.mark.asyncio
async def test_thread_usage_updates_do_not_snapshot_all_threads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    await state.load_or_create()
    for index in range(100):
        await state.record_thread_cumulative_usage(
            f"thread-{index}",
            TokenUsage(input_tokens=index, measured=True),
            deferred=False,
        )

    original_snapshot = state_module.collection_snapshot

    def reject_full_usage_snapshot(section: str, value: object) -> object:
        assert section != "thread_cumulative_usage"
        return original_snapshot(section, value)

    monkeypatch.setattr(state_module, "collection_snapshot", reject_full_usage_snapshot)
    await state.record_thread_cumulative_usage(
        "thread-50",
        TokenUsage(input_tokens=500, measured=True),
        deferred=False,
    )

    with sqlite3.connect(state.database_path) as connection:
        count = connection.execute(
            "SELECT count(*) FROM state_items WHERE section='thread_cumulative_usage'"
        ).fetchone()
        payload = connection.execute(
            """
            SELECT payload FROM state_items
            WHERE section='thread_cumulative_usage' AND item_key='thread-50'
            """
        ).fetchone()
    assert count == (100,)
    assert payload is not None
    assert json.loads(payload[0])["input_tokens"] == 500
    await state.close()


@pytest.mark.asyncio
async def test_graph_edge_insert_does_not_rewrite_unchanged_edge(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    await state.load_or_create()
    state.formalize_graph = {
        "order": ["a", "b", "c"],
        "edges": [["b", "c"]],
        "dependencies": {"a": [], "b": [], "c": ["b"]},
    }
    await state.save("formalize_graph")
    with sqlite3.connect(state.database_path) as connection:
        original_revision = connection.execute(
            """
            SELECT revision FROM graph_edges
            WHERE graph='formalize_graph' AND kind='dependency'
                AND source_id='b' AND target_id='c'
            """
        ).fetchone()

    state.formalize_graph = {
        "order": ["a", "b", "c"],
        "edges": [["a", "c"], ["b", "c"]],
        "dependencies": {"a": [], "b": [], "c": ["a", "b"]},
    }
    await state.save("formalize_graph")
    with sqlite3.connect(state.database_path) as connection:
        unchanged_revision = connection.execute(
            """
            SELECT revision FROM graph_edges
            WHERE graph='formalize_graph' AND kind='dependency'
                AND source_id='b' AND target_id='c'
            """
        ).fetchone()
        edge_count = connection.execute(
            "SELECT count(*) FROM graph_edges WHERE graph='formalize_graph'"
        ).fetchone()
    assert unchanged_revision == original_revision
    assert edge_count == (2,)
    await state.close()


@pytest.mark.asyncio
async def test_schema_v2_global_graphs_migrate_to_relational_rows(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    state = StateStore(config)
    await state.load_or_create()
    first, second = config.chapters
    state.source_dependency_tree = {
        "algorithm": "source-discovery",
        "revision": 1,
        "order": [first.id, second.id],
        "edges": [[first.id, second.id]],
        "dependencies": {first.id: [], second.id: [first.id]},
        "nodes": {
            first.id: {"dependencies": [], "summary": "root"},
            second.id: {"dependencies": [first.id], "summary": "dependent"},
        },
    }
    await state.save()
    await state.close()

    snapshot = read_full_snapshot(config.settings.state_dir)
    assert snapshot is not None
    legacy_header = {
        key: value
        for key, value in snapshot.items()
        if key not in {"documents", "work_units", "tasks", "source_issues"}
    }
    legacy_header["upstream_requests"] = {
        "request-1": {
            "id": "request-1",
            "status": "requested",
            "capability_key": "Book.LegacyCapability",
            "owner_chapter_id": first.id,
            "consumer_chapter_id": second.id,
            "consumer_path": "lean/Book/Chapter02.lean",
            "blocked_declaration": "Book.consumer",
            "needed_result": "A legacy capability",
        }
    }
    with sqlite3.connect(state.database_path) as connection, connection:
        connection.execute("DELETE FROM state_items")
        connection.execute("DELETE FROM graph_metadata")
        connection.execute("DELETE FROM graph_nodes")
        connection.execute("DELETE FROM graph_edges")
        connection.execute(
            "UPDATE globals SET payload=? WHERE key='state'",
            (json.dumps(legacy_header).encode(),),
        )
        connection.execute("PRAGMA user_version=2")

    StateDatabase(config.settings.state_dir).initialize()

    with sqlite3.connect(state.database_path) as connection:
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        header_payload = connection.execute(
            "SELECT payload FROM globals WHERE key='state'"
        ).fetchone()[0]
        edge_count = connection.execute(
            """
            SELECT count(*) FROM graph_edges
            WHERE graph='source_dependency_tree' AND source_id=? AND target_id=?
            """,
            (first.id, second.id),
        ).fetchone()[0]
        request_count = connection.execute(
            "SELECT count(*) FROM state_items WHERE section='upstream_requests'"
        ).fetchone()[0]
    assert version == SCHEMA_VERSION
    assert len(header_payload) < 10_000
    assert "source_dependency_tree" not in json.loads(header_payload)
    assert edge_count == 1
    assert request_count == 0
    migrated = read_full_snapshot(config.settings.state_dir)
    assert migrated is not None
    assert migrated["source_dependency_tree"]["edges"] == [[first.id, second.id]]
    assert "upstream_requests" not in migrated
    assert any(
        value["capability_key"] == "book.legacycapability"
        for value in migrated["capability_packages"].values()
    )


@pytest.mark.asyncio
async def test_schema_v3_adds_interface_invalidation_events(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path))
    state = StateStore(config)
    await state.load_or_create()
    await state.close()
    with sqlite3.connect(state.database_path) as connection, connection:
        connection.execute("DROP TABLE interface_invalidation_events")
        connection.execute("UPDATE meta SET schema_version=3 WHERE singleton=1")
        connection.execute("PRAGMA user_version=3")

    StateDatabase(config.settings.state_dir).initialize()

    with sqlite3.connect(state.database_path) as connection:
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        meta_version = connection.execute(
            "SELECT schema_version FROM meta WHERE singleton=1"
        ).fetchone()[0]
        event_table = connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name='interface_invalidation_events'"
        ).fetchone()
    assert version == SCHEMA_VERSION
    assert meta_version == SCHEMA_VERSION
    assert event_table == ("interface_invalidation_events",)


def test_legacy_json_checkpoint_imports_upstream_requests_into_packages_once(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / ".paf"
    state_dir.mkdir()
    request = {
        "id": "legacy-request",
        "capability_key": "Book.Legacy Bridge",
        "status": "requested",
        "consumer_chapter_id": "book/chapter-02",
        "blocked_declaration": "Book.consumer",
        "consumer_path": "lean/Book/Chapter02.lean",
        "needed_result": "A legacy bridge",
        "owner_paths": ["lean/Book/Chapter01.lean"],
        "created_at": "2026-08-21T00:00:00+00:00",
        "updated_at": "2026-08-21T00:00:00+00:00",
    }
    (state_dir / "state.json").write_text(
        json.dumps(
            {
                "version": 6,
                "created_at": "2026-08-21T00:00:00+00:00",
                "updated_at": "2026-08-21T00:00:00+00:00",
                "tasks": {},
                "upstream_requests": {"legacy-request": request},
            }
        ),
        encoding="utf-8",
    )

    database = StateDatabase(state_dir)
    database.initialize()
    first = database.load_package_state()
    database.initialize()
    second = database.load_package_state()

    imported = next(
        package
        for package in first.packages.values()
        if package.capability_key == "book.legacy bridge"
    )
    assert first.consumers_for(imported.id)[0].declaration == "Book.consumer"
    assert len(first.evidence_for(imported.id)) == 1
    assert second == first
    checkpoint = read_checkpoint(state_dir)
    assert checkpoint is not None
    assert "upstream_requests" not in checkpoint
    assert "upstream_request_imports" not in checkpoint


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


def test_project_canonicalizes_package_reservation_paths(tmp_path: Path) -> None:
    config_path = write_project(tmp_path)
    project = ProjectResolver(tmp_path).resolve(project=config_path)

    assert project.canonical_repository_path(tmp_path / "lean" / "Book" / "Chapter01.lean") == (
        "lean/Book/Chapter01.lean"
    )
    assert project.canonical_repository_path("lean\\Book\\Chapter01.lean") == (
        "lean/Book/Chapter01.lean"
    )
    with pytest.raises(ValueError, match="outside project repository"):
        project.canonical_repository_path(tmp_path.parent / "outside.lean")


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
    assert str(project) in capsys.readouterr().out.replace("\n", "")
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
    for command in (
        "status",
        "snapshot",
        "pause",
        "resume",
        "unblock",
        "stop",
        "wait",
        "rpc",
    ):
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


def test_normalized_checkpoint_keeps_source_order(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1, 2]"))
    template_book = config.books[0]
    template_chapters = config.chapters
    last_lexically = replace(template_book, id="z-first-in-source")
    first_lexically = replace(template_book, id="a-second-in-source")
    config = replace(
        config,
        books=(last_lexically, first_lexically),
        chapters=(
            replace(
                template_chapters[1],
                book_id=last_lexically.id,
                source_span=template_chapters[0].source_span,
            ),
            replace(
                template_chapters[0],
                book_id=last_lexically.id,
                source_span=template_chapters[1].source_span,
            ),
            replace(template_chapters[0], book_id=first_lexically.id),
        ),
    )
    state = StateStore(config)

    async def persist() -> None:
        await state.load_or_create()
        await state.close()

    asyncio.run(persist())
    checkpoint = read_checkpoint(config.settings.state_dir)

    assert checkpoint is not None
    assert [document["id"] for document in checkpoint["documents"]] == [
        last_lexically.id,
        first_lexically.id,
    ]
    assert [unit["id"] for unit in checkpoint["work_units"]] == [
        f"{last_lexically.id}/chapter-02",
        f"{last_lexically.id}/chapter-01",
        f"{first_lexically.id}/chapter-01",
    ]


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
