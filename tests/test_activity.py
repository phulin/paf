import json
from pathlib import Path

from lastlib_swarm.activity import ActivityStore, AgentActivity, systemic_errors


def test_summarizes_agent_events_and_persists_compact_activity(tmp_path: Path) -> None:
    activity = AgentActivity(run_id="run-1", chapter_id="book/chapter-01", stage="prove")
    activity.consume(
        {
            "type": "item.started",
            "item": {
                "id": "mcp-1",
                "type": "mcp_tool_call",
                "server": "lastlib_lean",
                "tool": "lean_diagnostic_messages",
                "status": "in_progress",
            },
        },
        workspace_root=tmp_path,
    )

    assert activity.current == "MCP lastlib_lean.lean_diagnostic_messages"

    activity.consume(
        {
            "type": "item.completed",
            "item": {
                "id": "mcp-1",
                "type": "mcp_tool_call",
                "server": "lastlib_lean",
                "tool": "lean_diagnostic_messages",
                "status": "failed",
                "error": None,
                "result": {
                    "content": [
                        {
                            "type": "text",
                            "text": "Failed to start Lean: No such file or directory: 'lake'",
                        }
                    ]
                },
            },
        },
        workspace_root=tmp_path,
    )
    activity.consume(
        {
            "type": "item.completed",
            "item": {
                "id": "edit-1",
                "type": "file_change",
                "status": "completed",
                "changes": [{"path": str(tmp_path / "lean" / "Section.lean"), "kind": "update"}],
            },
        },
        workspace_root=tmp_path,
    )

    store = ActivityStore(tmp_path / "logs")
    store.save(activity)
    loaded = ActivityStore(tmp_path / "logs").get("run-1")

    assert loaded is not None
    assert loaded.mcp_failures == 1
    assert loaded.files == ["lean/Section.lean"]
    assert systemic_errors([loaded]) == [(1, "Lean MCP cannot find lake")]
    persisted = json.loads(store.path("run-1").read_text(encoding="utf-8"))
    assert "active_items" not in persisted


def test_tracks_parallel_items_until_all_complete(tmp_path: Path) -> None:
    activity = AgentActivity(run_id="run", chapter_id="chapter", stage="formalize")
    for item_id, command in (("one", "first"), ("two", "second")):
        activity.consume(
            {
                "type": "item.started",
                "item": {
                    "id": item_id,
                    "type": "command_execution",
                    "command": command,
                    "status": "in_progress",
                },
            },
            workspace_root=tmp_path,
        )
    activity.consume(
        {
            "type": "item.completed",
            "item": {
                "id": "two",
                "type": "command_execution",
                "command": "second",
                "status": "completed",
                "exit_code": 0,
            },
        },
        workspace_root=tmp_path,
    )

    assert activity.current == "shell: first"


def test_preserves_the_complete_latest_agent_update(tmp_path: Path) -> None:
    activity = AgentActivity(run_id="run", chapter_id="chapter", stage="review")
    update = "first line\n" + "x" * 1_200 + "\nfinal line"

    activity.consume(
        {
            "type": "item.completed",
            "item": {"id": "message", "type": "agent_message", "text": update},
        },
        workspace_root=tmp_path,
    )

    assert activity.latest_summary == update
    assert len(activity.recent[-1].detail) == 800
    store = ActivityStore(tmp_path / "logs")
    store.save(activity)
    loaded = ActivityStore(tmp_path / "logs").get("run")
    assert loaded is not None
    assert loaded.latest_summary == update


def test_bare_exit_status_is_not_promoted_to_an_error(tmp_path: Path) -> None:
    activities = []
    for code in (2, 128):
        activity = AgentActivity(run_id=str(code), chapter_id="chapter", stage="prove")
        activity.consume(
            {
                "type": "item.completed",
                "item": {
                    "id": "command",
                    "type": "command_execution",
                    "command": "git command",
                    "status": "failed",
                    "exit_code": code,
                    "aggregated_output": "",
                },
            },
            workspace_root=tmp_path,
        )
        assert activity.failures == 1
        assert activity.latest_error == ""
        activities.append(activity)

    legacy = AgentActivity(run_id="legacy", chapter_id="chapter", stage="prove")
    legacy.latest_error = "exit 128"
    activities.append(legacy)
    assert systemic_errors(activities) == []


def test_failed_command_promotes_substantive_output(tmp_path: Path) -> None:
    activity = AgentActivity(run_id="run", chapter_id="chapter", stage="prove")
    activity.consume(
        {
            "type": "item.completed",
            "item": {
                "id": "command",
                "type": "command_execution",
                "command": "git command",
                "status": "failed",
                "exit_code": 128,
                "aggregated_output": "fatal: repository is unavailable",
            },
        },
        workspace_root=tmp_path,
    )

    assert activity.latest_error == "fatal: repository is unavailable"
    assert systemic_errors([activity]) == [(1, "fatal: repository is unavailable")]
