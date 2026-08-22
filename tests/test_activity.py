import json
import threading
from pathlib import Path

import pytest

import paf.activity as activity_module
from paf.activity import (
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


@pytest.mark.asyncio
async def test_async_activity_store_serializes_off_the_event_loop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    event_loop_thread = threading.get_ident()
    serialization_threads: list[int] = []
    original = activity_module.json.dumpb

    def observed_dumpb(value: object, *, indent: bool = False, sort_keys: bool = False) -> bytes:
        serialization_threads.append(threading.get_ident())
        return original(value, indent=indent, sort_keys=sort_keys)

    monkeypatch.setattr(activity_module.json, "dumpb", observed_dumpb)
    store = ActivityStore(tmp_path / "logs")

    activity = await store.start_async("run", "chapter", "formalize")
    activity.current = "persisted asynchronously"
    await store.save_async(activity)

    assert serialization_threads
    assert all(thread != event_loop_thread for thread in serialization_threads)
    persisted = ActivityStore(store.logs_dir).get("run")
    assert persisted is not None
    assert persisted.current == "persisted asynchronously"


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


def test_activity_records_codex_reconnects_without_promoting_them_to_failures(
    tmp_path: Path,
) -> None:
    activity = AgentActivity(run_id="run", chapter_id="chapter", stage="prove")
    message = (
        "Reconnecting... 2/5 (stream disconnected before completion: "
        "Incomplete response returned, reason: max_output_tokens)"
    )

    activity.consume(
        {
            "type": "error",
            "message": message,
            EVENT_TIMESTAMP_FIELD: "2026-08-20T20:38:13+00:00",
        },
        workspace_root=tmp_path,
    )

    assert activity.current == message
    assert activity.latest_error == ""
    assert activity.failures == 0
    assert activity.recent[-1].at == "2026-08-20T20:38:13+00:00"
    assert activity.recent[-1].status == "retrying"
    assert activity.recent[-1].title == message


def test_activity_records_top_level_codex_errors(tmp_path: Path) -> None:
    activity = AgentActivity(run_id="run", chapter_id="chapter", stage="prove")

    activity.consume(
        {"type": "error", "message": "stream failed permanently"},
        workspace_root=tmp_path,
    )

    assert activity.latest_error == "stream failed permanently"
    assert activity.recent[-1].status == "failed"
    assert activity.recent[-1].title == "Codex error"
    assert activity.recent[-1].detail == "stream failed permanently"


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
                "server": "example",
                "tool": "lookup",
                "status": "in_progress",
            },
        },
        workspace_root=tmp_path,
    )

    assert activity.current == "MCP example.lookup"

    activity.consume(
        {
            "type": "item.completed",
            "item": {
                "id": "mcp-1",
                "type": "mcp_tool_call",
                "server": "example",
                "tool": "lookup",
                "status": "failed",
                "error": None,
                "result": {
                    "content": [
                        {
                            "type": "text",
                            "text": "Failed to call external service",
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
    assert systemic_errors([loaded]) == [(1, "Failed to call external service")]
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


@pytest.mark.parametrize("bash", ["/bin/bash", "/usr/bin/bash"])
@pytest.mark.parametrize("quote", ["'", '"'])
def test_compacts_shell_command_wrappers_for_bash_paths_and_quotes(
    tmp_path: Path, bash: str, quote: str
) -> None:
    activity = AgentActivity(run_id="run", chapter_id="chapter", stage="formalize")
    activity.consume(
        {
            "type": "item.started",
            "item": {
                "id": "command",
                "type": "command_execution",
                "command": f"{bash} -lc {quote}lake build +Book.Chapter{quote}",
                "status": "in_progress",
            },
        },
        workspace_root=tmp_path,
    )

    assert activity.current == "shell: lake build +Book.Chapter"


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


def test_full_timeline_replay_does_not_truncate_agent_updates(tmp_path: Path) -> None:
    store = ActivityStore(tmp_path / "logs")
    update = json.dumps(
        {
            "complete": False,
            "summary": "working through the proof",
            "issues": ["x" * 1_200],
        }
    )
    log_path = store.logs_dir / "run.jsonl"
    log_path.parent.mkdir(parents=True)
    log_path.write_text(
        json.dumps(
            {
                "type": "item.completed",
                "item": {"id": "message", "type": "agent_message", "text": update},
            }
        )
        + "\n",
        encoding="utf-8",
    )

    compact = store.replay(
        "run",
        "chapter",
        "review",
        log_path,
        workspace_root=tmp_path,
        cache=False,
    )
    assert compact is not None
    assert len(compact.recent[-1].detail) == 800

    full = store.replay(
        "run",
        "chapter",
        "review",
        log_path,
        workspace_root=tmp_path,
        maximum_events=None,
        detail_limit=None,
        cache=False,
    )
    assert full is not None
    assert full.recent[-1].detail == update
    assert json.loads(full.recent[-1].detail)["issues"] == ["x" * 1_200]


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
