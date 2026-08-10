from __future__ import annotations

import os
from pathlib import Path

import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from lastlib_swarm.codex import LEAN_MCP_PROOF_TOOLS, lean_mcp_executable


@pytest.mark.skipif(
    os.environ.get("LASTLIB_SWARM_MCP_SMOKE") != "1",
    reason="set LASTLIB_SWARM_MCP_SMOKE=1 to launch the real Lean MCP server",
)
@pytest.mark.asyncio
async def test_real_lean_mcp_advertises_required_tools() -> None:
    project = Path(os.environ.get("LASTLIB_SWARM_MCP_PROJECT", Path.cwd() / "lean")).resolve()
    environment = {
        **os.environ,
        "LEAN_PROJECT_PATH": str(project),
        "LEAN_LOG_LEVEL": "NONE",
        "PYTHONWARNINGS": "ignore",
    }
    parameters = StdioServerParameters(
        command=str(lean_mcp_executable()),
        cwd=project,
        env=environment,
    )

    async with (
        stdio_client(parameters) as (read_stream, write_stream),
        ClientSession(read_stream, write_stream) as session,
    ):
        await session.initialize()
        response = await session.list_tools()
        available = {tool.name for tool in response.tools}
        diagnostic_file = os.environ.get("LASTLIB_SWARM_MCP_DIAGNOSTIC_FILE")
        diagnostic = (
            await session.call_tool(
                "lean_diagnostic_messages",
                {"file_path": diagnostic_file},
            )
            if diagnostic_file
            else None
        )

    assert set(LEAN_MCP_PROOF_TOOLS) <= available
    if diagnostic is not None:
        assert not diagnostic.isError
