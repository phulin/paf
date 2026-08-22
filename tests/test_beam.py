from __future__ import annotations

from pathlib import Path

import pytest

from paf.beam import BeamSession
from paf.config import load_config
from tests.support import write_project


@pytest.mark.asyncio
async def test_beam_session_uses_per_run_control_and_shared_bundle_dirs(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    assert config.backend is not None

    session = await BeamSession.start(
        command=config.backend.beam_command,
        project=tmp_path / config.backend.project,
        state_dir=config.settings.state_dir,
        run_id="formalize-book-01",
        timeout_seconds=5,
    )
    try:
        assert session.process.returncode is None
        assert session.environment["BEAM_CONTROL_DIR"] == str(
            config.settings.state_dir / "beam" / "control" / "formalize-book-01"
        )
        assert session.environment["BEAM_BUNDLE_DIR"] == str(
            config.settings.state_dir / "beam" / "bundles"
        )
        assert session.environment["PAF_BEAM_COMMAND"] == config.backend.beam_command
        assert session.control_dir.is_dir()
    finally:
        await session.close()

    assert session.process.returncode is not None
    assert not session.control_dir.exists()
    assert session.log_path.is_file()
    assert session.log_path.read_text(encoding="utf-8").startswith('{\n  "result"')


@pytest.mark.asyncio
async def test_beam_session_rejects_a_missing_executable(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="Lean Beam executable was not found"):
        await BeamSession.start(
            command=str(tmp_path / "missing-beam"),
            project=tmp_path,
            state_dir=tmp_path / ".paf",
            run_id="run",
            timeout_seconds=1,
        )
