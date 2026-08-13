import json
from pathlib import Path

import pytest

import lastlib_swarm.activity as activity_module
from lastlib_swarm.activity import (
    ActivityStore,
    AgentActivity,
    shorten_book_paths,
    systemic_errors,
)


def test_activity_store_throttles_reconstructible_sidecar_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = [0.0]
    monkeypatch.setattr(activity_module.time, "monotonic", lambda: clock[0])
    store = ActivityStore(tmp_path / "logs")
    activity = AgentActivity(run_id="run", chapter_id="chapter", stage="formalize")
    store.save(activity)

    activity.current = "updated in memory"
    activity.sequence = 1
    clock[0] = 0.5
    store.save_throttled(activity, interval=1.0)
    persisted = ActivityStore(tmp_path / "logs").get("run")
    assert persisted is not None
    assert persisted.current != "updated in memory"
    assert store.get("run") is activity

    clock[0] = 1.0
    store.save_throttled(activity, interval=1.0)
    persisted = ActivityStore(tmp_path / "logs").get("run")
    assert persisted is not None
    assert persisted.current == "updated in memory"


def test_shortens_book_path_in_long_lake_trace() -> None:
    lean_paths = ":".join(
        f"/home/example/project/lean/.lake/packages/package-{index}/.lake/build/lib/lean"
        for index in range(20)
    )
    source = (
        "/home/example/project/lean/LastLib/Book02FiniteExtensionsOfLocalFields/"
        "Chapter07/Section04FiniteResidueFields.lean"
    )

    shortened = shorten_book_paths(f"trace: .> LEAN_PATH={lean_paths} {source}")

    assert shortened.endswith("[Book 2 Chap 7 Sec 4: Finite Residue Fields]")


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


def test_pretty_prints_lean_mcp_queries_and_results(tmp_path: Path) -> None:
    activity = AgentActivity(run_id="run", chapter_id="chapter", stage="prove")
    arguments = {
        "file_path": (
            "LastLib/Book02FiniteExtensionsOfLocalFields/Chapter07/"
            "Section04FiniteResidueFields.lean"
        ),
        "line": 79,
        "timeout_s": 1800,
    }
    started = {
        "type": "item.started",
        "item": {
            "id": "goal",
            "type": "mcp_tool_call",
            "server": "lastlib_lean",
            "tool": "lean_goal",
            "arguments": arguments,
            "status": "in_progress",
        },
    }
    activity.consume(started, workspace_root=tmp_path)

    query = activity.recent[-1].detail
    assert query.startswith("Query:\n{\n")
    assert '  "file_path": "[Book 2 Chap 7 Sec 4: Finite Residue Fields]"' in query
    assert '\n  "line": 79,' in query

    activity.consume(
        {
            "type": "item.completed",
            "item": {
                **started["item"],
                "status": "completed",
                "result": {
                    "content": [{"type": "text", "text": "duplicate content"}],
                    "structured_content": {
                        "line_context": "  sorry",
                        "goals_before": ["x : Nat\n⊢ x = x"],
                        "goals_after": [],
                    },
                },
            },
        },
        workspace_root=tmp_path,
    )

    detail = activity.recent[-1].detail
    assert detail.startswith("Result:\n{\n")
    assert '  "goals_before": [' in detail
    assert "duplicate content" not in detail
    assert activity.mcp_calls == 1


def test_lean_mcp_result_falls_back_from_null_structured_content(tmp_path: Path) -> None:
    activity = AgentActivity(run_id="run", chapter_id="chapter", stage="fixup")
    activity.consume(
        {
            "type": "item.completed",
            "item": {
                "id": "diagnostics",
                "type": "mcp_tool_call",
                "server": "lastlib_lean",
                "tool": "lean_diagnostic_messages",
                "arguments": {"file_path": "Broken.lean"},
                "status": "failed",
                "error": None,
                "result": {
                    "content": [
                        {
                            "type": "text",
                            "text": "Error executing tool: elaboration failed",
                        }
                    ],
                    "structured_content": None,
                },
            },
        },
        workspace_root=tmp_path,
    )

    assert activity.recent[-1].detail.endswith("Result:\nError executing tool: elaboration failed")
    assert activity.latest_error == "Error executing tool: elaboration failed"


def test_large_lean_mcp_query_does_not_hide_result(tmp_path: Path) -> None:
    activity = AgentActivity(run_id="run", chapter_id="chapter", stage="prove")
    activity.consume(
        {
            "type": "item.completed",
            "item": {
                "id": "attempt",
                "type": "mcp_tool_call",
                "server": "lastlib_lean",
                "tool": "lean_multi_attempt",
                "arguments": {
                    "file_path": "Proof.lean",
                    "line": 10,
                    "snippets": ["exact " + "x" * 500 for _ in range(5)],
                },
                "status": "completed",
                "result": {
                    "structured_content": {
                        "items": [{"snippet": "exact h", "proof_status": "complete"}]
                    }
                },
            },
        },
        workspace_root=tmp_path,
    )

    detail = activity.recent[-1].detail
    assert detail.startswith("Query:\n")
    assert "\n\nResult:\n" in detail
    assert '"proof_status": "complete"' in detail
    assert len(detail) <= 800


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


def test_compacts_shell_commands_and_successful_completions(tmp_path: Path) -> None:
    activity = AgentActivity(run_id="run", chapter_id="chapter", stage="formalize")
    command = (
        "/bin/bash -lc \"sed -n '1,80p' "
        "/tmp/swarm/lean/LastLib/Book05LocalClassFieldTheory/Chapter07/"
        'Section07WhyFrobeniusIsCanonical.lean"'
    )
    started = {
        "type": "item.started",
        "item": {
            "id": "command",
            "type": "command_execution",
            "command": command,
            "status": "in_progress",
        },
    }
    activity.consume(started, workspace_root=tmp_path)

    assert activity.current == (
        "shell: sed -n '1,80p' [Book 5 Chap 7 Sec 7: Why Frobenius Is Canonical]"
    )

    activity.consume(
        {
            "type": "item.completed",
            "item": {
                **started["item"],
                "status": "completed",
                "exit_code": 0,
            },
        },
        workspace_root=tmp_path,
    )

    assert activity.recent[-1].title == "done"


def test_compacts_dependency_paths_in_commands_and_file_changes(tmp_path: Path) -> None:
    dependency = (
        tmp_path
        / "lean"
        / "LastLib"
        / "Book05LocalClassFieldTheory"
        / "Chapter07"
        / "Dependencies.lean"
    )
    activity = AgentActivity(run_id="run", chapter_id="chapter", stage="formalize")
    activity.consume(
        {
            "type": "item.started",
            "item": {
                "id": "command",
                "type": "command_execution",
                "command": f"/bin/bash -lc 'lake env lean {dependency}'",
                "status": "in_progress",
            },
        },
        workspace_root=tmp_path,
    )
    assert activity.current == "shell: lake env lean [Book 5 Chap 7 Dependencies]"

    activity.consume(
        {
            "type": "item.completed",
            "item": {
                "id": "edit",
                "type": "file_change",
                "status": "completed",
                "changes": [{"path": str(dependency), "kind": "update"}],
            },
        },
        workspace_root=tmp_path,
    )

    assert activity.files == ["[Book 5 Chap 7 Dependencies]"]
    assert activity.recent[-1].title == "success"
    assert activity.recent[-1].detail == ""


def test_successful_file_change_only_names_file_when_started(tmp_path: Path) -> None:
    activity = AgentActivity(run_id="run", chapter_id="chapter", stage="formalize")
    item = {
        "id": "edit",
        "type": "file_change",
        "changes": [{"path": str(tmp_path / "lean" / "Section.lean"), "kind": "update"}],
    }
    activity.consume(
        {"type": "item.started", "item": {**item, "status": "in_progress"}},
        workspace_root=tmp_path,
    )
    activity.consume(
        {"type": "item.completed", "item": {**item, "status": "completed"}},
        workspace_root=tmp_path,
    )

    started, completed = activity.recent[-2:]
    assert started.title == "editing lean/Section.lean"
    assert completed.title == "success"
    assert completed.detail == ""


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
