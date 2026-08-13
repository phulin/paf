import asyncio
import json
import os
import threading
from dataclasses import replace
from pathlib import Path

import pytest

import lastlib_swarm.isolation as isolation_module
import lastlib_swarm.scheduler as scheduler_module
from lastlib_swarm.codex import ValidationResult
from lastlib_swarm.config import load_config
from lastlib_swarm.isolation import FuseOverlayIsolation, fuse_overlay_available
from lastlib_swarm.models import Chapter, Stage
from lastlib_swarm.scheduler import Orchestrator
from lastlib_swarm.state import StateStore
from tests.support import write_project

pytestmark = pytest.mark.skipif(
    not fuse_overlay_available(), reason="fuse-overlayfs is unavailable"
)


def fuse_manager(config_path: Path) -> tuple[FuseOverlayIsolation, Chapter]:
    config = load_config(config_path)
    settings = replace(config.settings, isolation="fuse-overlay", max_agents=2)
    return FuseOverlayIsolation(settings), config.chapters[0]


@pytest.mark.asyncio
async def test_fuse_overlay_manifest_scans_do_not_block_event_loop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager, chapter = fuse_manager(write_project(tmp_path, chapters="chapters = [1]"))
    await manager.prepare()
    workspace = await manager.acquire("responsive-collect")
    event_loop_thread = threading.get_ident()
    manifest_threads: list[int] = []

    def tracked_manifest(root: Path, *, excluded: tuple[str, ...]) -> dict[object, object]:
        del root, excluded
        manifest_threads.append(threading.get_ident())
        return {}

    monkeypatch.setattr(isolation_module, "tree_manifest", tracked_manifest)
    try:
        result = await workspace.collect(chapter)
    finally:
        await workspace.close()
        await manager.close()

    assert result.accepted
    assert len(manifest_threads) == 2
    assert all(thread != event_loop_thread for thread in manifest_threads)


@pytest.mark.asyncio
async def test_fuse_overlay_rejects_out_of_scope_changes(tmp_path: Path) -> None:
    manager, chapter = fuse_manager(write_project(tmp_path, chapters="chapters = [1]"))
    await manager.prepare()
    workspace = await manager.acquire("outside")
    try:
        allowed = workspace.root / "lean" / "Book" / "Chapter01.lean"
        allowed.parent.mkdir(parents=True)
        allowed.write_text("theorem allowed : True := by trivial\n", encoding="utf-8")
        direct = workspace.root / "lean" / "Book" / "Chapter01" / "Section01.lean"
        direct.parent.mkdir(parents=True)
        direct.write_text("theorem direct : True := by trivial\n", encoding="utf-8")
        nested = workspace.root / "lean" / "Book" / "Chapter01" / "Part" / "Section02.lean"
        nested.parent.mkdir(parents=True)
        nested.write_text("theorem nested : True := by trivial\n", encoding="utf-8")
        (workspace.root / "unexpected.txt").write_text("not allowed\n", encoding="utf-8")

        result = await workspace.collect(chapter)
    finally:
        await workspace.close()
        await manager.close()

    assert not result.accepted
    assert result.out_of_scope_paths == ("unexpected.txt",)
    assert not (tmp_path / "lean" / "Book" / "Chapter01.lean").exists()
    assert not (tmp_path / "unexpected.txt").exists()


@pytest.mark.asyncio
async def test_active_source_generation_is_immutable_when_live_repo_changes(
    tmp_path: Path,
) -> None:
    manager, chapter = fuse_manager(write_project(tmp_path, chapters="chapters = [1]"))
    infrastructure = tmp_path / "orchestrator.py"
    infrastructure.write_text("version = 1\n", encoding="utf-8")
    await manager.prepare()
    workspace = await manager.acquire("immutable-source")
    try:
        snapshot = workspace.base / infrastructure.relative_to(tmp_path)
        mounted = workspace.root / infrastructure.relative_to(tmp_path)
        assert snapshot.read_text(encoding="utf-8") == "version = 1\n"
        assert mounted.read_text(encoding="utf-8") == "version = 1\n"
        assert snapshot.stat().st_ino != infrastructure.stat().st_ino

        infrastructure.write_text("version = 2\n", encoding="utf-8")
        allowed = workspace.root / "lean" / "Book" / "Chapter01.lean"
        allowed.parent.mkdir(parents=True)
        allowed.write_text("theorem allowed : True := by trivial\n", encoding="utf-8")

        result = await workspace.collect(chapter)
    finally:
        await workspace.close()
        await manager.close()

    assert result.accepted
    assert result.changed_paths == ("lean/Book/Chapter01.lean",)
    assert infrastructure.read_text(encoding="utf-8") == "version = 2\n"
    assert (tmp_path / "lean" / "Book" / "Chapter01.lean").exists()


@pytest.mark.asyncio
async def test_fuse_overlay_reclaims_dead_orchestrator_roots(tmp_path: Path) -> None:
    manager, _ = fuse_manager(write_project(tmp_path, chapters="chapters = [1]"))
    stale = manager.parent / f"{manager.identity}-999999999"
    (stale / "slots" / "0000-dead" / "merged").mkdir(parents=True)
    (stale / "marker").write_text("stale\n", encoding="utf-8")

    await manager.prepare()
    try:
        assert not stale.exists()
    finally:
        await manager.close()


@pytest.mark.asyncio
async def test_fuse_overlay_lazily_detaches_busy_stale_mount(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager, _ = fuse_manager(write_project(tmp_path, chapters="chapters = [1]"))
    stale = manager.parent / f"{manager.identity}-999999999"
    merged = stale / "slots" / "0000-dead" / "merged"
    merged.mkdir(parents=True)
    commands: list[tuple[str, ...]] = []

    async def fake_run(*command: str) -> None:
        commands.append(command)
        if command[1] == "-u":
            raise RuntimeError("fusermount3 failed (1): Device or resource busy")

    original_ismount = os.path.ismount
    monkeypatch.setattr(
        os.path,
        "ismount",
        lambda path: Path(path) == merged or original_ismount(path),
    )
    monkeypatch.setattr(manager, "_run", fake_run)

    await manager._clean_stale_roots()

    assert commands == [
        ("fusermount3", "-u", str(merged)),
        ("fusermount3", "-uz", str(merged)),
    ]
    assert not stale.exists()


@pytest.mark.asyncio
async def test_fuse_overlay_reclaims_disconnected_mount(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager, _ = fuse_manager(write_project(tmp_path, chapters="chapters = [1]"))
    stale = manager.parent / f"{manager.identity}-999999999"
    merged = stale / "slots" / "0000-dead" / "merged"
    merged.mkdir(parents=True)
    commands: list[tuple[str, ...]] = []

    async def fake_run(*command: str) -> None:
        commands.append(command)

    monkeypatch.setattr(isolation_module, "_mount_points", lambda: {merged})
    monkeypatch.setattr(os.path, "ismount", lambda _path: False)
    monkeypatch.setattr(manager, "_run", fake_run)

    await manager._clean_stale_roots()

    assert commands == [("fusermount3", "-u", str(merged))]
    assert not stale.exists()


@pytest.mark.asyncio
async def test_fuse_overlay_imports_scope_and_rejects_a_stale_writer(tmp_path: Path) -> None:
    manager, chapter = fuse_manager(write_project(tmp_path, chapters="chapters = [1]"))
    await manager.prepare()
    first = await manager.acquire("first")
    stale = await manager.acquire("stale")
    first_file = first.root / "lean" / "Book" / "Chapter01.lean"
    stale_file = stale.root / "lean" / "Book" / "Chapter01.lean"
    first_file.parent.mkdir(parents=True)
    stale_file.parent.mkdir(parents=True)
    first_file.write_text("theorem first : True := by trivial\n", encoding="utf-8")
    expected_mtime = 1_700_000_000_123_456_789
    os.utime(first_file, ns=(expected_mtime, expected_mtime))
    stale_file.write_text("theorem stale : True := by trivial\n", encoding="utf-8")
    try:
        accepted = await first.collect(chapter)
        rejected = await stale.collect(chapter)
    finally:
        await first.close()
        await stale.close()
        await manager.close()

    assert accepted.accepted
    assert not rejected.accepted
    assert "fresh generation" in rejected.error
    assert (
        (tmp_path / "lean" / "Book" / "Chapter01.lean")
        .read_text(encoding="utf-8")
        .startswith("theorem first")
    )
    assert (tmp_path / "lean" / "Book" / "Chapter01.lean").stat().st_mtime_ns == expected_mtime


@pytest.mark.asyncio
async def test_agent_cache_writes_are_discarded_and_coordinator_publishes_delta(
    tmp_path: Path,
) -> None:
    config_path = write_project(tmp_path, chapters="chapters = [1]")
    seed = tmp_path / "lean" / ".lake" / "packages" / "dependency.olean"
    seed.parent.mkdir(parents=True)
    seed.write_bytes(b"shared dependency")
    manager, chapter = fuse_manager(config_path)
    await manager.prepare()
    writer = await manager.acquire("cache-writer")
    pinned = await manager.acquire("cache-pinned")
    fresh = None
    try:
        artifact = "lean/.lake/build/lib/lean/Book/Chapter01.olean"
        writer_artifact = writer.root / artifact
        writer_artifact.parent.mkdir(parents=True)
        writer_artifact.write_bytes(b"new chapter artifact")

        assert (writer.root / seed.relative_to(tmp_path)).read_bytes() == b"shared dependency"
        assert (pinned.root / seed.relative_to(tmp_path)).read_bytes() == b"shared dependency"
        result = await writer.collect(chapter)
        await writer.close()
        build = await manager.acquire_build("coordinator")
        coordinator_artifact = build.root / artifact
        coordinator_artifact.parent.mkdir(parents=True, exist_ok=True)
        coordinator_artifact.write_bytes(b"coordinator artifact")
        promoted = await build.finish(succeeded=True, publish=True)
        fresh = await manager.acquire("cache-fresh")
        assert result.accepted
        assert result.promoted_cache_paths == ()
        assert not (pinned.root / artifact).exists()
        assert (fresh.root / artifact).read_bytes() == b"coordinator artifact"
        assert promoted == (artifact,)
        assert (pinned.cache / seed.relative_to(tmp_path)).stat().st_ino == (
            fresh.cache / seed.relative_to(tmp_path)
        ).stat().st_ino
        assert (tmp_path / artifact).read_bytes() == b"coordinator artifact"
        assert writer.cache_generation == pinned.cache_generation == 0
        assert fresh.cache_generation == 1
    finally:
        await writer.close()
        await pinned.close()
        if fresh is not None:
            await fresh.close()
        await manager.close()


@pytest.mark.asyncio
async def test_cache_delta_publication_keeps_active_agent_snapshots_immutable(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    manager = FuseOverlayIsolation(replace(config.settings, isolation="fuse-overlay", max_agents=3))
    await manager.prepare()
    pinned = await manager.acquire("cache-pinned")
    first_fresh = None
    second_fresh = None
    try:
        first_only = "lean/.lake/build/first.olean"
        second_only = "lean/.lake/build/second.olean"
        first_build = await manager.acquire_build("first")
        first_target = first_build.root / first_only
        first_target.parent.mkdir(parents=True, exist_ok=True)
        first_target.write_bytes(b"first")
        await first_build.finish(succeeded=True, publish=True)
        first_fresh = await manager.acquire("cache-first-fresh")

        second_build = await manager.acquire_build("second")
        second_target = second_build.root / second_only
        second_target.write_bytes(b"second")
        await second_build.finish(succeeded=True, publish=True)
        second_fresh = await manager.acquire("cache-second-fresh")

        assert not (pinned.root / first_only).exists()
        assert not (pinned.root / second_only).exists()
        assert (first_fresh.root / first_only).read_bytes() == b"first"
        assert not (first_fresh.root / second_only).exists()
        assert (second_fresh.root / first_only).read_bytes() == b"first"
        assert (second_fresh.root / second_only).read_bytes() == b"second"
    finally:
        await pinned.close()
        if first_fresh is not None:
            await first_fresh.close()
        if second_fresh is not None:
            await second_fresh.close()
        await manager.close()


@pytest.mark.asyncio
async def test_orchestrator_merges_then_fixup_builds_in_main_worktree(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_path = write_project(tmp_path, chapters="chapters = [1]")
    fake_codex = tmp_path / "fake-codex"
    fake_codex.write_text(
        """#!/usr/bin/env python3
import json
import pathlib
import sys

sys.stdin.read()
target = pathlib.Path("lean/Book/Chapter01.lean")
target.parent.mkdir(parents=True, exist_ok=True)
target.write_text("theorem isolated : True := by trivial\\n", encoding="utf-8")
artifact = pathlib.Path("lean/.lake/build/lib/lean/Book/Chapter01.olean")
artifact.parent.mkdir(parents=True, exist_ok=True)
artifact.write_bytes(b"compiled")
report = {"changed": True, "complete": True,
          "summary": "isolated", "issues": []}
print(json.dumps({"type": "item.completed", "item": {
    "type": "agent_message", "text": json.dumps(report)}}))
""",
        encoding="utf-8",
    )
    fake_codex.chmod(0o755)
    config = load_config(config_path)
    config = replace(
        config,
        settings=replace(
            config.settings,
            codex_bin=str(fake_codex),
            isolation="fuse-overlay",
        ),
        chapters=(replace(config.chapters[0], build_command="true"),),
    )
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)
    await orchestrator.prepare()

    async def coordinator_validation(
        _config: object,
        _chapter: object,
        *,
        workspace_root: Path | None = None,
        **_kwargs: object,
    ) -> ValidationResult:
        assert workspace_root is not None
        assert (workspace_root / "lean" / "Book" / "Chapter01.lean").read_text(
            encoding="utf-8"
        ) == "theorem isolated : True := by trivial\n"
        artifact = workspace_root / "lean" / ".lake" / "build" / "coordinator.marker"
        artifact.parent.mkdir(parents=True, exist_ok=True)
        artifact.write_bytes(b"built in main")
        return ValidationResult(True, 0, "ok")

    monkeypatch.setattr(scheduler_module, "validate", coordinator_validation)

    assert await orchestrator.run_stage(Stage.FORMALIZE)
    assert (tmp_path / "lean" / "Book" / "Chapter01.lean").read_text(
        encoding="utf-8"
    ) == "theorem isolated : True := by trivial\n"
    run = state.task(config.chapters[0].id, Stage.FORMALIZE).runs[0]
    assert run.isolation is not None
    assert run.isolation["accepted"] is True
    assert run.isolation["promoted_cache_paths"] == []
    assert not (tmp_path / "lean" / ".lake" / "build" / "coordinator.marker").exists()

    assert await orchestrator.run_stage(Stage.FIXUP)
    assert (tmp_path / "lean" / ".lake" / "build" / "coordinator.marker").read_bytes() == (
        b"built in main"
    )
    assert json.loads((config.settings.state_dir / "state.json").read_text())["tasks"]
    await orchestrator.shutdown()


@pytest.mark.asyncio
async def test_fuse_overlay_supports_a_large_concurrent_slot_pool(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    slot_count = int(os.environ.get("LASTLIB_SWARM_STRESS_SLOTS", "12"))
    manager = FuseOverlayIsolation(
        replace(config.settings, isolation="fuse-overlay", max_agents=slot_count)
    )
    await manager.prepare()
    workspaces = await asyncio.gather(
        *(manager.acquire(f"stress-{index:03d}") for index in range(slot_count))
    )

    assert len({workspace.root for workspace in workspaces}) == slot_count
    assert len({workspace.base for workspace in workspaces}) == 1
    assert len({workspace.cache for workspace in workspaces}) == 1

    await asyncio.gather(*(workspace.close() for workspace in workspaces))
    await manager.close()
    assert not manager.root.exists()
