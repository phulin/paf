import asyncio
import json
import os
from dataclasses import replace
from pathlib import Path

import pytest

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
async def test_fuse_overlay_rejects_out_of_scope_changes(tmp_path: Path) -> None:
    manager, chapter = fuse_manager(write_project(tmp_path, chapters="chapters = [1]"))
    await manager.prepare()
    workspace = await manager.acquire("outside")
    try:
        allowed = workspace.root / "lean" / "Book" / "Chapter01.lean"
        allowed.parent.mkdir(parents=True)
        allowed.write_text("theorem allowed : True := by trivial\n", encoding="utf-8")
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
async def test_lake_cache_is_shared_pinned_and_promoted_without_copying(tmp_path: Path) -> None:
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
        result = await writer.collect(chapter, promote_cache=True)
        await writer.close()
        fresh = await manager.acquire("cache-fresh")
        assert result.accepted
        assert result.promoted_cache_paths == (artifact,)
        assert not (pinned.root / artifact).exists()
        assert (fresh.root / artifact).read_bytes() == b"new chapter artifact"
        assert (pinned.cache / seed.relative_to(tmp_path)).stat().st_ino == (
            fresh.cache / seed.relative_to(tmp_path)
        ).stat().st_ino
        assert not (tmp_path / artifact).exists()
        assert writer.cache_generation == pinned.cache_generation == 0
        assert fresh.cache_generation == 1
    finally:
        await writer.close()
        await pinned.close()
        if fresh is not None:
            await fresh.close()
        await manager.close()


@pytest.mark.asyncio
async def test_concurrent_cache_promotions_merge_unrelated_artifacts(tmp_path: Path) -> None:
    manager, chapter = fuse_manager(write_project(tmp_path, chapters="chapters = [1]"))
    await manager.prepare()
    first = await manager.acquire("cache-first")
    second = await manager.acquire("cache-second")
    fresh = None
    try:
        first_only = "lean/.lake/build/first.olean"
        second_only = "lean/.lake/build/second.olean"
        conflict = "lean/.lake/build/shared.trace"
        for workspace, relative, contents in (
            (first, first_only, b"first"),
            (first, conflict, b"first wins"),
            (second, second_only, b"second"),
            (second, conflict, b"stale second"),
        ):
            target = workspace.root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(contents)

        first_result = await first.collect(chapter, promote_cache=True)
        second_result = await second.collect(chapter, promote_cache=True)
        await first.close()
        fresh = await manager.acquire("cache-merged")
        assert set(first_result.promoted_cache_paths) == {first_only, conflict}
        assert second_result.promoted_cache_paths == (second_only,)
        assert (fresh.root / first_only).read_bytes() == b"first"
        assert (fresh.root / second_only).read_bytes() == b"second"
        assert (fresh.root / conflict).read_bytes() == b"first wins"
    finally:
        await first.close()
        await second.close()
        if fresh is not None:
            await fresh.close()
        await manager.close()


@pytest.mark.asyncio
async def test_orchestrator_runs_agent_and_build_inside_fuse_overlay(tmp_path: Path) -> None:
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
report = {"changed": True, "complete": True, "needs_repair": False,
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

    assert await orchestrator.run_stage(Stage.FORMALIZE)
    assert (tmp_path / "lean" / "Book" / "Chapter01.lean").read_text(
        encoding="utf-8"
    ) == "theorem isolated : True := by trivial\n"
    run = state.task(config.chapters[0].id, Stage.FORMALIZE).runs[0]
    assert run.isolation is not None
    assert run.isolation["accepted"] is True
    assert run.isolation["promoted_cache_paths"] == [
        "lean/.lake/build/lib/lean/Book/Chapter01.olean"
    ]
    assert not (tmp_path / "lean" / ".lake" / "build").exists()
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

    await asyncio.gather(*(workspace.close() for workspace in workspaces))
    await manager.close()
    assert not manager.root.exists()
