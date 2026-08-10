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
