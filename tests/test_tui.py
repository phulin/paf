from pathlib import Path

import pytest

from lastlib_swarm.config import load_config
from lastlib_swarm.scheduler import Orchestrator
from lastlib_swarm.state import StateStore, TokenUsage
from lastlib_swarm.tui import SwarmApp, format_count, format_usage
from tests.support import write_project


def test_formats_measured_token_spend_without_double_counting_cache() -> None:
    usage = TokenUsage(
        input_tokens=1_250_000,
        cached_input_tokens=1_000_000,
        output_tokens=50_000,
        reasoning_output_tokens=20_000,
        measured=True,
    )

    rendered = format_usage(usage)

    assert "1.30m" in rendered
    assert "cached 1.00m" in rendered
    assert format_count(999) == "999"


@pytest.mark.asyncio
async def test_dashboard_runs_an_operation_and_exits(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    orchestrator = Orchestrator(config, state)

    async def operation() -> bool:
        return True

    app = SwarmApp(orchestrator, operation, label="test")
    async with app.run_test() as pilot:
        await pilot.pause(1.2)

    assert app.result
