import json
from pathlib import Path

import pytest

import lastlib_swarm.activity as activity_module
from lastlib_swarm.activity import (
    EVENT_TIMESTAMP_FIELD,
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


def test_activity_replay_can_retain_a_full_timeline_without_replacing_cache(
    tmp_path: Path,
) -> None:
    store = ActivityStore(tmp_path / "logs")
    compact = store.start("run", "chapter", "formalize")
    log_path = store.logs_dir / "run.jsonl"
    events = [
        {
            "type": "item.completed",
            "item": {
                "id": f"event-{index}",
                "type": f"timeline_event_{index}",
                "status": "completed",
            },
        }
        for index in range(100)
    ]
    log_path.write_text(
        "".join(json.dumps(event) + "\n" for event in events),
        encoding="utf-8",
    )

    replayed = store.replay(
        "run",
        "chapter",
        "formalize",
        log_path,
        workspace_root=tmp_path,
        maximum_events=10_000,
        cache=False,
    )

    assert replayed is not None
    assert len(replayed.recent) == 100
    assert replayed.recent[0].sequence == 1
    assert store.get("run") is compact

    bounded = store.replay(
        "run",
        "chapter",
        "formalize",
        log_path,
        workspace_root=tmp_path,
        maximum_events=25,
        cache=False,
    )
    assert bounded is not None
    assert len(bounded.recent) == 25
    assert bounded.recent[0].sequence == 76


def test_activity_replay_preserves_recorded_event_timestamps(tmp_path: Path) -> None:
    store = ActivityStore(tmp_path / "logs")
    log_path = store.logs_dir / "run.jsonl"
    recorded_at = [
        "2026-08-14T10:00:00+00:00",
        "2026-08-14T10:07:30+00:00",
    ]
    events = [
        {"type": "turn.started", EVENT_TIMESTAMP_FIELD: recorded_at[0]},
        {
            "type": "item.completed",
            "item": {"id": "message", "type": "agent_message", "text": "still working"},
            EVENT_TIMESTAMP_FIELD: recorded_at[1],
        },
    ]
    log_path.parent.mkdir(parents=True)
    log_path.write_text(
        "".join(json.dumps(event) + "\n" for event in events),
        encoding="utf-8",
    )

    replayed = store.replay(
        "run",
        "chapter",
        "formalize",
        log_path,
        workspace_root=tmp_path,
        cache=False,
    )

    assert replayed is not None
    assert [entry.at for entry in replayed.recent] == recorded_at
    assert replayed.updated_at == recorded_at[-1]


def test_activity_replay_recovers_legacy_timestamps_from_sidecar(tmp_path: Path) -> None:
    store = ActivityStore(tmp_path / "logs")
    activity = store.start("run", "chapter", "formalize")
    events = [
        {"type": "turn.started"},
        {
            "type": "item.completed",
            "item": {"id": "message", "type": "agent_message", "text": "still working"},
        },
    ]
    recorded_at = [
        "2026-08-14T10:00:00+00:00",
        "2026-08-14T10:07:30+00:00",
    ]
    for event, at in zip(events, recorded_at, strict=True):
        activity.consume(event, workspace_root=tmp_path, at=at)
    store.save(activity)
    log_path = store.logs_dir / "run.jsonl"
    log_path.write_text(
        "".join(json.dumps(event) + "\n" for event in events),
        encoding="utf-8",
    )

    replayed = ActivityStore(store.logs_dir).replay(
        "run",
        "chapter",
        "formalize",
        log_path,
        workspace_root=tmp_path,
        cache=False,
    )

    assert replayed is not None
    assert [entry.at for entry in replayed.recent] == recorded_at


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

    assert activity.current == "Lean diagnostics"

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


def test_summarizes_lean_mcp_queries_and_results_on_one_line(tmp_path: Path) -> None:
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
    assert query == "Q goal [Book 2 Chap 7 Sec 4: Finite Residue Fields]:79"
    assert "\n" not in query

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
    assert detail == "R 1→0 goals"
    assert "duplicate content" not in detail
    assert "\n" not in detail
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

    assert activity.recent[-1].detail == (
        "Q diagnostics Broken.lean · R Error executing tool: elaboration failed"
    )
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
    assert detail.startswith("Q 5 attempts:")
    assert " · R 1 attempts: 1 complete · worked: exact h" in detail
    assert "\n" not in detail
    assert len(detail) <= 800


@pytest.mark.parametrize(
    ("tool", "arguments", "expected"),
    [
        (
            "lean_prepare_dependencies",
            {"file_paths": ["A.lean", "B.lean"]},
            "Q prepare 2 files: A.lean, B.lean",
        ),
        (
            "lean_diagnostic_messages",
            {
                "file_path": "A.lean",
                "start_line": 4,
                "end_line": 9,
                "severity": "error",
            },
            "Q diagnostics A.lean:4-9 · error",
        ),
        (
            "lean_hover_info",
            {"file_path": "A.lean", "line": 4, "column": 7},
            "Q hover A.lean:4:7",
        ),
        (
            "lean_declaration_file",
            {"file_path": "A.lean", "symbol": "Nat.succ", "context_lines": 5},
            "Q declaration Nat.succ in A.lean · ±5 lines",
        ),
        (
            "lean_local_search",
            {"query": "foo", "limit": 4},
            "Q local search “foo” · max 4",
        ),
        (
            "lean_goal",
            {"file_path": "A.lean", "line": 4},
            "Q goal A.lean:4",
        ),
        (
            "lean_term_goal",
            {"file_path": "A.lean", "line": 4, "column": 7},
            "Q term goal A.lean:4:7",
        ),
        (
            "lean_completions",
            {"file_path": "A.lean", "line": 4, "column": 7, "max_completions": 12},
            "Q completions A.lean:4:7 · max 12",
        ),
        (
            "lean_multi_attempt",
            {"file_path": "A.lean", "line": 4, "snippets": ["simp", "omega"]},
            "Q 2 attempts: simp, omega at A.lean:4",
        ),
        (
            "lean_code_actions",
            {"file_path": "A.lean", "line": 4},
            "Q code actions A.lean:4",
        ),
    ],
)
def test_summarizes_each_enabled_lean_mcp_query(
    tmp_path: Path, tool: str, arguments: dict[str, object], expected: str
) -> None:
    activity = AgentActivity(run_id="run", chapter_id="chapter", stage="prove")
    activity.consume(
        {
            "type": "item.started",
            "item": {
                "id": tool,
                "type": "mcp_tool_call",
                "server": "lastlib_lean",
                "tool": tool,
                "arguments": arguments,
                "status": "in_progress",
            },
        },
        workspace_root=tmp_path,
    )

    assert activity.recent[-1].detail == expected
    assert "\n" not in activity.recent[-1].detail


@pytest.mark.parametrize(
    ("tool", "value", "expected"),
    [
        (
            "lean_prepare_dependencies",
            {"prepared": ["A.lean", "B.lean"], "stale": ["B.lean"]},
            "R 2 prepared · 1 stale",
        ),
        (
            "lean_diagnostic_messages",
            {
                "success": False,
                "items": [
                    {"severity": "error", "message": "unknown identifier", "line": 7, "column": 3},
                    {"severity": "warning", "message": "unused variable", "line": 8, "column": 1},
                ],
            },
            "R 1 error · 1 warning · L7:3 unknown identifier",
        ),
        (
            "lean_hover_info",
            {"symbol": "Nat.succ", "info": "Nat → Nat", "diagnostics": []},
            "R Nat.succ: Nat → Nat",
        ),
        (
            "lean_declaration_file",
            {
                "file_path": "Mathlib/Data/Nat.lean",
                "content": "large",
                "start_line": 10,
                "end_line": 20,
                "total_lines": 100,
            },
            "R Mathlib/Data/Nat.lean L10-20/100",
        ),
        (
            "lean_local_search",
            {"items": [{"name": "Foo"}, {"name": "Bar"}]},
            "R 2 matches: Foo, Bar",
        ),
        (
            "lean_goal",
            {"goals_before": ["⊢ A", "⊢ B"], "goals_after": ["⊢ B"]},
            "R 2→1 goals",
        ),
        (
            "lean_term_goal",
            {"line_context": "exact ?_", "expected_type": "Nat"},
            "R expected Nat",
        ),
        (
            "lean_completions",
            {"items": [{"label": "foo"}, {"label": "bar"}]},
            "R 2 completions: foo, bar",
        ),
        (
            "lean_multi_attempt",
            {
                "items": [
                    {"snippet": "exact h", "goals": [], "proof_status": "Completed"},
                    {"snippet": "simp", "goals": ["⊢ A"]},
                    {"snippet": "omega", "diagnostics": [{"severity": "error"}]},
                ]
            },
            "R 3 attempts: 1 complete, 1 open, 1 failed · worked: exact h",
        ),
        (
            "lean_code_actions",
            {"actions": [{"title": "Try this: simp"}, {"title": "Add missing cases"}]},
            "R 2 actions: Try this: simp, Add missing cases",
        ),
    ],
)
def test_summarizes_each_enabled_lean_mcp_result(
    tmp_path: Path, tool: str, value: dict[str, object], expected: str
) -> None:
    activity = AgentActivity(run_id="run", chapter_id="chapter", stage="prove")
    activity.consume(
        {
            "type": "item.completed",
            "item": {
                "id": tool,
                "type": "mcp_tool_call",
                "server": "lastlib_lean",
                "tool": tool,
                "status": "completed",
                "result": {"structured_content": value},
            },
        },
        workspace_root=tmp_path,
    )

    assert activity.recent[-1].detail == expected
    assert "\n" not in activity.recent[-1].detail


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
