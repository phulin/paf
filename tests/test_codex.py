import asyncio
import json
import os
import signal
from collections.abc import Callable
from dataclasses import replace
from io import BytesIO
from pathlib import Path

import pytest

import paf.codex as codex_module
from paf.activity import EVENT_TIMESTAMP_FIELD
from paf.codex import (
    DIAGNOSTIC_REVIEW_ROLE,
    PACKAGE_STEWARD_ROLE,
    PACKAGE_WORKER_ROLE,
    REPORT_SCHEMAS,
    WARNING_REVIEW_ROLE,
    CodexExecutor,
    FatalCodexInvocationError,
    _bounded_feedback,
    _capacity_resume_delay,
    _complete_lines,
    _is_capacity_failure,
    _is_fatal_invocation_failure,
    _record_jsonl_line,
    _rollout_usage,
    _tail_rollout_usage,
    count_placeholders,
    declaration_uses_placeholder,
    declaration_uses_placeholder_in_chapter,
    lean_mcp_executable,
    lean_mcp_path,
    proof_target_chunk,
    proof_target_spans,
    proof_targets,
    render_prompt,
    unexpected_lean_warnings,
    validate,
)
from paf.config import load_config
from paf.models import Stage
from paf.state import StateStore, TaskStatus, TokenUsage
from paf.state_db import read_full_snapshot
from tests.support import write_project


def test_record_jsonl_line_wraps_raw_process_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(codex_module, "activity_timestamp", lambda: "2026-08-18T01:02:03Z")
    log = BytesIO()

    event, received_at = _record_jsonl_line(
        log,
        b"ERROR codex_core::tools::router: apply_patch failed",
        terminated=True,
    )

    persisted = json.loads(log.getvalue())
    assert event == persisted
    assert received_at == "2026-08-18T01:02:03Z"
    assert persisted == {
        "type": "paf.raw_output",
        "text": "ERROR codex_core::tools::router: apply_patch failed",
        "terminated": True,
        EVENT_TIMESTAMP_FIELD: "2026-08-18T01:02:03Z",
    }


def test_record_jsonl_line_deduplicates_structured_mcp_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(codex_module, "activity_timestamp", lambda: "2026-08-18T01:02:03Z")
    source = {
        "type": "item.completed",
        "item": {
            "type": "mcp_tool_call",
            "result": {
                "content": [{"type": "text", "text": '{"items": []}'}],
                "structured_content": {"items": []},
            },
        },
    }
    log = BytesIO()

    event, _ = _record_jsonl_line(log, json.dumps(source).encode(), terminated=True)

    persisted = json.loads(log.getvalue())
    assert "content" not in persisted["item"]["result"]
    assert persisted["item"]["result"]["structured_content"] == {"items": []}
    assert event["item"]["result"]["content"] == source["item"]["result"]["content"]


def test_report_schemas_contain_only_fields_used_by_each_active_agent() -> None:
    expected = {
        "package_steward": {
            "complete",
            "summary",
            "issues",
            "diagnosis",
            "placement_decision",
            "scope_expansion_requests",
            "plan_revision",
            "completed_step_assessments",
            "worker_assignments",
            "package_dependency_requests",
            "child_packages",
            "consumer_assessments",
            "disposition",
            "remaining_work",
        },
        "package_worker": {
            "complete",
            "summary",
            "issues",
            "step_id",
            "changed_declarations",
            "changed_paths",
            "commit_id",
            "focused_validation",
            "remaining_gap",
            "new_evidence",
        },
        "discover": {"complete", "summary", "issues", "source_dependencies"},
        "formalize": {"complete", "summary", "issues", "source_issues"},
        "review": {"complete", "summary", "issues", "source_issues"},
        "diagnostic_review": {
            "complete",
            "summary",
            "issues",
            "source_issues",
        },
        "warning_cleanup": {
            "complete",
            "summary",
            "issues",
            "source_issues",
        },
        "proof_review": {
            "complete",
            "summary",
            "issues",
            "source_issues",
            "finding_assessments",
        },
        "prove": {
            "complete",
            "disposition",
            "summary",
            "issues",
            "source_issues",
            "failed_attempts",
            "blocker_refs",
        },
    }
    assert set(REPORT_SCHEMAS) == set(expected)
    for key, fields in expected.items():
        assert set(REPORT_SCHEMAS[key]["properties"]) == fields
        assert set(REPORT_SCHEMAS[key]["required"]) == fields


def test_report_schema_avoids_unsupported_codex_keywords() -> None:
    def mappings(value: object) -> list[dict[str, object]]:
        if isinstance(value, dict):
            return [value, *(item for child in value.values() for item in mappings(child))]
        if isinstance(value, list):
            return [item for child in value for item in mappings(child)]
        return []

    assert all(
        "uniqueItems" not in value
        for schema in REPORT_SCHEMAS.values()
        for value in mappings(schema)
    )


def test_report_schema_closes_every_nested_object_shape() -> None:
    def mappings(value: object) -> list[dict[str, object]]:
        if isinstance(value, dict):
            return [value, *(item for child in value.values() for item in mappings(child))]
        if isinstance(value, list):
            return [item for child in value for item in mappings(child)]
        return []

    def is_object_shape(value: dict[str, object]) -> bool:
        raw_type = value.get("type")
        return raw_type == "object" or (isinstance(raw_type, list) and "object" in raw_type)

    object_shapes = [
        value
        for schema in REPORT_SCHEMAS.values()
        for value in mappings(schema)
        if is_object_shape(value)
    ]
    assert object_shapes
    assert all(value.get("additionalProperties") is False for value in object_shapes)


def test_extracts_api_equivalent_usage() -> None:
    usage = TokenUsage.from_event(
        {
            "type": "turn.completed",
            "usage": {
                "input_tokens": 120,
                "cached_input_tokens": 40,
                "output_tokens": 30,
                "reasoning_output_tokens": 12,
            },
        }
    )

    assert usage is not None
    assert usage.total_tokens == 150
    assert usage.cached_input_tokens == 40
    assert usage.reasoning_output_tokens == 12
    assert usage.measured


def test_extracts_live_rollout_usage() -> None:
    usage = _rollout_usage(
        {
            "type": "event_msg",
            "payload": {
                "type": "token_count",
                "info": {
                    "total_token_usage": {
                        "input_tokens": 400,
                        "cached_input_tokens": 300,
                        "output_tokens": 25,
                        "reasoning_output_tokens": 10,
                        "total_tokens": 425,
                    }
                },
            },
        }
    )

    assert usage is not None
    assert usage.total_tokens == 425
    assert usage.cached_input_tokens == 300


def test_frames_complete_lines_without_discarding_a_partial_record() -> None:
    pending = bytearray(b'{"first":')

    lines = _complete_lines(pending, b'1}\n{"second":2}\n{"third":')

    assert lines == (b'{"first":1}', b'{"second":2}')
    assert pending == b'{"third":'


@pytest.mark.asyncio
async def test_rollout_tail_skips_large_non_usage_records_before_json_decode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from paf import codex as codex_module

    async def run_inline(function: Callable[..., object], *args: object) -> object:
        return function(*args)

    rollout = tmp_path / "rollout.jsonl"
    irrelevant = b'{"type":"response_item","payload":"' + b"x" * (2 * 1024 * 1024) + b'"}\n'
    token_count = json.dumps(
        {
            "type": "event_msg",
            "payload": {
                "type": "token_count",
                "info": {
                    "total_token_usage": {
                        "input_tokens": 400,
                        "cached_input_tokens": 300,
                        "output_tokens": 25,
                    }
                },
            },
        }
    ).encode()
    rollout.write_bytes(irrelevant + token_count + b"\n")
    decoded_sizes: list[int] = []
    original_loads = codex_module.json.loads

    def recording_loads(value: bytes) -> object:
        decoded_sizes.append(len(value))
        return original_loads(value)

    monkeypatch.setattr(codex_module, "_codex_rollout", lambda _thread_id: rollout)
    monkeypatch.setattr(codex_module.asyncio, "to_thread", run_inline)
    monkeypatch.setattr(codex_module.json, "loads", recording_loads)
    monkeypatch.setattr(codex_module, "USAGE_POLL_SECONDS", 0)
    stop = asyncio.Event()
    updates: list[TokenUsage] = []

    async def update(usage: TokenUsage) -> None:
        updates.append(usage)
        stop.set()

    await _tail_rollout_usage("thread", stop, update)

    assert [usage.total_tokens for usage in updates] == [425]
    assert decoded_sizes == [len(token_count)]


def test_counts_only_lean_code_placeholders(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    chapter_dir = tmp_path / "lean" / "Book" / "Chapter01"
    chapter_dir.mkdir(parents=True)
    (chapter_dir / "Section.lean").write_text(
        """
-- sorry
/- admit /- sorry -/ -/
def message := "sorry"
theorem first : True := by sorry
theorem second : True := by admit
""",
        encoding="utf-8",
    )

    assert count_placeholders(tmp_path, chapter) == 2


def test_proof_targets_are_declaration_scoped_and_chunked_by_placeholder_count(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    path = tmp_path / "lean" / "Book" / "Chapter01.lean"
    path.parent.mkdir(parents=True)
    path.write_text(
        "-- theorem ignored : True := by sorry\n"
        'def message := "sorry"\n'
        + "\n".join(f"theorem target{i} : True := by sorry" for i in range(1, 10))
        + "\n",
        encoding="utf-8",
    )

    targets = proof_targets(tmp_path, chapter)
    chunks = []
    remaining = targets
    while remaining:
        chunk = proof_target_chunk(remaining, 4)
        chunks.append(chunk)
        remaining = remaining[len(chunk) :]

    assert [target.declaration for target in targets] == [f"target{i}" for i in range(1, 10)]
    assert [(target.line, target.end_line) for target in targets[:2]] == [(3, 3), (4, 4)]
    assert [[hole.line for hole in target.obligations] for target in targets[:2]] == [[3], [4]]
    assert [sum(target.placeholder_count for target in chunk) for chunk in chunks] == [4, 4, 1]
    assert len({target.fingerprint for target in targets}) == 9


def test_proof_chunk_does_not_split_one_declaration(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    path = tmp_path / "lean" / "Book" / "Chapter01.lean"
    path.parent.mkdir(parents=True)
    path.write_text(
        "theorem large : True := by\n  have a : True := by sorry\n  sorry\n"
        "theorem later : True := by sorry\n",
        encoding="utf-8",
    )

    targets = proof_targets(tmp_path, chapter)

    assert [target.placeholder_count for target in proof_target_chunk(targets, 1)] == [2]


def test_proof_hole_context_is_anchored_to_the_placeholder_line(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    path = tmp_path / "lean" / "Book" / "Chapter01.lean"
    path.parent.mkdir(parents=True)
    path.write_text(
        "/-- A doc comment whose final line must not label the proof hole.\n"
        "· misleading doc text\n"
        "-/\n"
        "theorem standalone : True := by\n"
        "  sorry\n"
        "theorem inline : True := by\n"
        "  have result : True := by sorry\n"
        "  exact result\n",
        encoding="utf-8",
    )

    targets = proof_targets(tmp_path, chapter)

    assert [target.obligations[0].context for target in targets] == [
        "sorry",
        "have result : True := by sorry",
    ]


def test_proof_target_spans_refresh_after_an_assigned_proof_expands(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    path = tmp_path / "lean" / "Book" / "Chapter01.lean"
    path.parent.mkdir(parents=True)
    path.write_text(
        "theorem assigned : True := by sorry\ntheorem later : True := by sorry\n",
        encoding="utf-8",
    )
    assigned = proof_targets(tmp_path, chapter)[0]
    path.write_text(
        "theorem assigned : True := by\n  have h : True := by trivial\n  exact h\n"
        "theorem later : True := by sorry\n",
        encoding="utf-8",
    )

    refreshed = proof_target_spans(tmp_path, chapter, (assigned,))[0]

    assert refreshed.fingerprint == assigned.fingerprint
    assert (refreshed.line, refreshed.end_line, refreshed.placeholder_count) == (1, 3, 0)


def test_proof_obligation_identity_survives_an_earlier_hole_being_solved(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    path = tmp_path / "lean" / "Book" / "Chapter01.lean"
    path.parent.mkdir(parents=True)
    path.write_text(
        "theorem assigned : True := by\n"
        "  have left : True := by sorry\n"
        "  have right : True := by sorry\n"
        "  exact right\n",
        encoding="utf-8",
    )
    original = proof_targets(tmp_path, chapter)[0]
    second_fingerprint = original.obligations[1].fingerprint
    path.write_text(
        path.read_text(encoding="utf-8").replace("by sorry", "by trivial", 1),
        encoding="utf-8",
    )

    remaining = proof_targets(tmp_path, chapter)[0]

    assert remaining.placeholder_count == 1
    assert remaining.obligations[0].fingerprint == second_fingerprint


def test_unnamed_examples_are_independent_proof_targets(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    path = tmp_path / "lean" / "Book" / "Chapter01.lean"
    path.parent.mkdir(parents=True)
    path.write_text(
        "theorem named : True := by sorry\nexample : True := by sorry\n",
        encoding="utf-8",
    )

    targets = proof_targets(tmp_path, chapter)

    assert [(target.declaration, target.placeholder_count) for target in targets] == [
        ("named", 1),
        ("example #1", 1),
    ]


def test_proof_prompt_contains_only_the_assigned_chunk(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    config.stages[Stage.PROVE].prompt.write_text(
        "# Prove {book_title}\n\n## Mission\n\nProve the assigned holes.\n\n"
        "## Working method\n\nUse checked evidence.\n",
        encoding="utf-8",
    )
    executor = CodexExecutor(config, StateStore(config))
    assigned = {
        "path": "lean/Book/Chapter01.lean",
        "declaration": "Book.target",
        "line": 17,
        "end_line": 24,
        "placeholder_count": 2,
        "fingerprint": "abc123",
        "obligations": [
            {"ordinal": 1, "line": 20, "context": "left := by", "fingerprint": "hole1"},
            {"ordinal": 2, "line": 23, "context": "right := by", "fingerprint": "hole2"},
        ],
    }

    prompt = executor.build_prompt(chapter, Stage.PROVE, proof_targets=[assigned])

    assert "Current merged-source target" in prompt
    assert "exactly 1 declaration containing\n2 proof holes" in prompt
    assert "`Book.target`" in prompt
    assert "H1 at line 20: `left := by`" in prompt
    assert "H2 at line 23: `right := by`" in prompt
    assert "There are no unassigned placeholders" in prompt
    assert prompt.index("## Mission") < prompt.index("## Current merged-source target")
    assert prompt.index("## Current merged-source target") < prompt.index("## Working method")
    assert "Resolve every\nlisted hole" in prompt


def test_executor_uses_machine_readable_codex_mode(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    executor = CodexExecutor(config, StateStore(config))

    command = executor.command(Stage.PROVE)

    assert command[:2] == ["codex", "exec"]
    assert "--json" in command
    assert "--output-schema" in command
    assert "--dangerously-bypass-approvals-and-sandbox" in command
    assert "--approve-for-me" not in command
    assert "--sandbox" not in command
    assert "--skip-git-repo-check" not in command
    assert command[command.index("--model") + 1] == "gpt-5.6-luna"
    assert 'model_reasoning_effort="xhigh"' in command
    isolated = tmp_path / "isolated"
    isolated_command = executor.command(Stage.PROVE, isolated)
    assert "--skip-git-repo-check" in isolated_command
    overrides = {
        isolated_command[index + 1].split("=", 1)[0]: isolated_command[index + 1].split("=", 1)[1]
        for index, item in enumerate(isolated_command[:-1])
        if item == "--config"
    }
    assert overrides["mcp_servers.paf_lean.command"] == f'"{lean_mcp_executable()}"'
    assert json.loads(overrides["mcp_servers.paf_lean.args"]) == [
        "-m",
        "paf.lean_mcp",
    ]
    assert overrides["mcp_servers.paf_lean.cwd"] == f'"{isolated / "lean"}"'
    assert json.loads(overrides["mcp_servers.paf_lean.env.PATH"]) == lean_mcp_path()
    assert "mcp_servers.paf_lean.env.LEAN_MCP_SCRATCH_SLOTS" not in overrides
    assert "mcp_servers.paf_lean.env.LEAN_MCP_PREWARM_FILES" not in overrides
    assert "lean_diagnostic_messages" in overrides["mcp_servers.paf_lean.enabled_tools"]
    assert "lean_prepare_dependencies" in overrides["mcp_servers.paf_lean.enabled_tools"]
    assert "lean_multi_attempt" in overrides["mcp_servers.paf_lean.enabled_tools"]
    assert "lean_file_outline" not in overrides["mcp_servers.paf_lean.enabled_tools"]
    assert "lean_build" not in overrides["mcp_servers.paf_lean.enabled_tools"]
    assert render_prompt(
        "Chapter {chapter_number_padded}: {chapter_title}", config.chapters[0]
    ) == ("Chapter 01: First chapter")

    review_command = executor.command(Stage.REVIEW)
    assert "--dangerously-bypass-approvals-and-sandbox" in review_command
    assert "--sandbox" not in review_command
    review_overrides = {
        review_command[index + 1].split("=", 1)[0]: review_command[index + 1].split("=", 1)[1]
        for index, item in enumerate(review_command[:-1])
        if item == "--config"
    }
    assert "lean_diagnostic_messages" in review_overrides["mcp_servers.paf_lean.enabled_tools"]
    assert "lean_prepare_dependencies" in review_overrides["mcp_servers.paf_lean.enabled_tools"]
    assert "lean_code_actions" in review_overrides["mcp_servers.paf_lean.enabled_tools"]
    assert "lean_file_outline" not in review_overrides["mcp_servers.paf_lean.enabled_tools"]
    assert "lean_multi_attempt" not in review_overrides["mcp_servers.paf_lean.enabled_tools"]
    assert "lean_build" not in review_overrides["mcp_servers.paf_lean.enabled_tools"]
    formalize_command = executor.command(Stage.FORMALIZE, isolated)
    formalize_overrides = {
        formalize_command[index + 1].split("=", 1)[0]: formalize_command[index + 1].split("=", 1)[1]
        for index, item in enumerate(formalize_command[:-1])
        if item == "--config"
    }
    assert "lean_diagnostic_messages" in formalize_overrides["mcp_servers.paf_lean.enabled_tools"]
    assert "lean_code_actions" in formalize_overrides["mcp_servers.paf_lean.enabled_tools"]
    assert "lean_file_outline" not in formalize_overrides["mcp_servers.paf_lean.enabled_tools"]
    assert "lean_multi_attempt" not in formalize_overrides["mcp_servers.paf_lean.enabled_tools"]
    assert "lean_goal" not in formalize_overrides["mcp_servers.paf_lean.enabled_tools"]

    discover_command = executor.command(Stage.DISCOVER)
    assert not any("mcp_servers.paf_lean" in item for item in discover_command)
    assert discover_command[discover_command.index("--model") + 1] == "gpt-5.6-luna"
    assert 'model_reasoning_effort="xhigh"' in discover_command
    discover_prompt = executor.build_prompt(config.chapters[0], Stage.DISCOVER)
    assert "None. Discovery is strictly read-only" in discover_prompt
    assert "You may edit only these paths" not in discover_prompt

    resumed = executor.command(Stage.FORMALIZE, isolated, resume_thread_id="capacity-thread")
    assert resumed[:3] == ["codex", "exec", "resume"]
    assert "capacity-thread" in resumed
    assert "--json" in resumed
    assert "--output-schema" in resumed
    assert "--cd" not in resumed
    resumed_review = executor.command(Stage.REVIEW, isolated, resume_thread_id="review-thread")
    assert "--dangerously-bypass-approvals-and-sandbox" in resumed_review


def test_discovery_catalog_contains_only_previous_chapters(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path))
    executor = CodexExecutor(config, StateStore(config))
    first, second = config.work_units

    first_prompt = executor.build_prompt(first, Stage.DISCOVER)
    second_prompt = executor.build_prompt(second, Stage.DISCOVER)

    assert "No earlier chapters are available." in first_prompt
    assert f"`{first.id}`" not in first_prompt
    assert f"`{second.id}`" not in first_prompt
    assert f"`{first.id}`" in second_prompt
    assert f"`{second.id}`" not in second_prompt


def test_executor_uses_stage_specific_model_and_reasoning_effort(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    stages = dict(config.stages)
    stages[Stage.REVIEW] = replace(
        stages[Stage.REVIEW], model="gpt-5.6-sol", reasoning_effort="high"
    )
    config = replace(config, stages=stages)
    executor = CodexExecutor(config, StateStore(config))

    command = executor.command(Stage.REVIEW)

    assert command[command.index("--model") + 1] == "gpt-5.6-sol"
    assert 'model_reasoning_effort="high"' in command


def test_warning_cleanup_uses_dedicated_minimal_disturbance_prompt(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    executor = CodexExecutor(config, StateStore(config))

    prompt = executor.build_prompt(
        config.work_units[0],
        Stage.REVIEW,
        role=WARNING_REVIEW_ROLE,
        feedback="warning: Book/Chapter01.lean:4:2: unused tactic",
    )

    assert prompt.startswith("# Clean Lean warnings")
    assert "Preserve all existing declarations" in prompt
    assert "do not replace, simplify, reorganize, or re-prove an existing proof" in prompt
    assert "Resolve every supplied warning" in prompt
    assert "PAF validation diagnostics to repair" in prompt


@pytest.mark.asyncio
async def test_executor_selects_a_distinct_schema_for_each_active_agent(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    executor = CodexExecutor(config, StateStore(config))
    await executor.prepare()

    def schema_path(command: list[str]) -> Path:
        return Path(command[command.index("--output-schema") + 1])

    selected = {
        "discover": schema_path(executor.command(Stage.DISCOVER)),
        "formalize": schema_path(executor.command(Stage.FORMALIZE)),
        "review": schema_path(executor.command(Stage.REVIEW)),
        "proof_review": schema_path(executor.command(Stage.REVIEW, feedback="evidence")),
        "diagnostic_review": schema_path(
            executor.command(
                Stage.REVIEW,
                feedback="diagnostic",
                role=DIAGNOSTIC_REVIEW_ROLE,
            )
        ),
        "warning_cleanup": schema_path(
            executor.command(
                Stage.REVIEW,
                feedback="warning",
                role=WARNING_REVIEW_ROLE,
            )
        ),
        "prove": schema_path(executor.command(Stage.PROVE)),
        "package_steward": schema_path(executor.command(Stage.PROVE, role=PACKAGE_STEWARD_ROLE)),
        "package_worker": schema_path(executor.command(Stage.PROVE, role=PACKAGE_WORKER_ROLE)),
    }

    assert len(set(selected.values())) == len(selected)
    for key, path in selected.items():
        assert json.loads(path.read_text(encoding="utf-8")) == REPORT_SCHEMAS[key]
    assert not (config.settings.state_dir / "agent-report.schema.json").exists()


@pytest.mark.parametrize("role", [DIAGNOSTIC_REVIEW_ROLE, WARNING_REVIEW_ROLE])
def test_diagnostic_review_has_proof_capable_lean_tools(tmp_path: Path, role: str) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    executor = CodexExecutor(config, StateStore(config))

    command = executor.command(
        Stage.REVIEW,
        feedback="diagnostic",
        role=role,
    )
    overrides = {
        command[index + 1].split("=", 1)[0]: json.loads(command[index + 1].split("=", 1)[1])
        for index, item in enumerate(command[:-1])
        if item == "--config"
    }

    assert "lean_goal" in overrides["mcp_servers.paf_lean.enabled_tools"]
    assert "lean_multi_attempt" in overrides["mcp_servers.paf_lean.enabled_tools"]


def test_lean_mcp_does_not_prewarm_files(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    chapter_root = tmp_path / "lean" / "Book" / "Chapter01"
    chapter_root.mkdir(parents=True)
    aggregator = tmp_path / "lean" / "Book" / "Chapter01.lean"
    dependency = chapter_root / "Dependencies.lean"
    proof = chapter_root / "Proof.lean"
    other = chapter_root / "Other.lean"
    aggregator.write_text(
        "import Book.Chapter01.Proof\nimport Book.Chapter01.Other\n",
        encoding="utf-8",
    )
    dependency.write_text("import Mathlib\n", encoding="utf-8")
    proof.write_text(
        "import Book.Chapter01.Dependencies\n\ntheorem target : True := by sorry\n",
        encoding="utf-8",
    )
    other.write_text("import Book.Chapter01.Dependencies\n", encoding="utf-8")

    command = CodexExecutor(config, StateStore(config)).command(
        Stage.PROVE,
        chapter=chapter,
        feedback="Coordinator diagnostic: lean/Book/Chapter01/Other.lean:1:1",
    )
    overrides = {
        command[index + 1].split("=", 1)[0]: command[index + 1].split("=", 1)[1]
        for index, item in enumerate(command[:-1])
        if item == "--config"
    }
    assert "mcp_servers.paf_lean.env.LEAN_MCP_PREWARM_FILES" not in overrides
    assert "mcp_servers.paf_lean.env.LEAN_MCP_SCRATCH_SLOTS" not in overrides


@pytest.mark.parametrize(
    "event",
    [
        {"type": "error", "message": "Selected model is at capacity. Please try again."},
        {
            "type": "error",
            "message": "Error running remote compact task: Selected model is at capacity.",
        },
        {
            "type": "turn.failed",
            "error": {"message": "Selected model is at capacity. Please try again."},
        },
        {
            "type": "error",
            "message": (
                "exceeded retry limit, last status: 429 Too Many Requests, "
                "request id: example-request"
            ),
        },
        {
            "type": "turn.failed",
            "error": {"message": "unexpected HTTP/2 429 response from upstream"},
        },
    ],
)
def test_detects_capacity_failures(event: dict[str, object]) -> None:
    assert _is_capacity_failure(event)


def test_does_not_retry_unrelated_turn_failures() -> None:
    assert not _is_capacity_failure(
        {"type": "turn.failed", "error": {"message": "authentication failed"}}
    )


def test_detects_invalid_output_schema_as_fatal() -> None:
    assert _is_fatal_invocation_failure(
        {
            "type": "turn.failed",
            "error": {
                "message": (
                    "Invalid schema for response_format 'codex_output_schema': "
                    "'uniqueItems' is not permitted."
                )
            },
        }
    )


def test_capacity_retry_schedule_uses_capped_exponential_backoff() -> None:
    assert [_capacity_resume_delay(15, 120, attempt) for attempt in range(1, 11)] == [
        15,
        30,
        60,
        120,
        120,
        120,
        120,
        120,
        120,
        120,
    ]


def test_lean_mcp_path_finds_elan_outside_inherited_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    elan_bin = tmp_path / "elan" / "bin"
    elan_bin.mkdir(parents=True)
    (elan_bin / "lake").touch()
    monkeypatch.setenv("ELAN_HOME", str(tmp_path / "elan"))
    monkeypatch.setenv("PATH", "/usr/bin")

    assert lean_mcp_path().split(os.pathsep)[0] == str(elan_bin)


def test_approve_for_me_is_not_combined_with_explicit_sandbox(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    config = replace(
        config,
        settings=replace(
            config.settings,
            bypass_approvals_and_sandbox=False,
            approve_for_me=True,
        ),
    )
    command = CodexExecutor(config, StateStore(config)).command(Stage.PROVE)

    assert "--approve-for-me" in command
    assert "--sandbox" not in command
    assert "--dangerously-bypass-approvals-and-sandbox" not in command


def test_review_uses_write_sandbox_when_full_access_is_disabled(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    config = replace(
        config,
        settings=replace(
            config.settings,
            bypass_approvals_and_sandbox=False,
            sandbox="workspace-write",
        ),
    )
    command = CodexExecutor(config, StateStore(config)).command(Stage.REVIEW)

    assert "--sandbox" in command
    assert command[command.index("--sandbox") + 1] == "workspace-write"
    assert "--dangerously-bypass-approvals-and-sandbox" not in command

    resumed = CodexExecutor(config, StateStore(config)).command(
        Stage.REVIEW, resume_thread_id="review-thread"
    )
    assert resumed[:3] == ["codex", "exec", "resume"]
    assert "--sandbox" not in resumed
    assert 'sandbox_mode="workspace-write"' in resumed


def test_review_command_enables_lean_mcp(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    executor = CodexExecutor(config, StateStore(config))

    command = executor.command(Stage.REVIEW)
    assert any("mcp_servers.paf_lean.command" in item for item in command)
    assert any("lean_diagnostic_messages" in item for item in command)


def test_declaration_placeholder_check_is_targeted_to_the_named_declaration(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = config.chapters[0]
    path = tmp_path / "lean" / "Book" / "Chapter01.lean"
    path.parent.mkdir(parents=True)
    path.write_text(
        "theorem solved : True := by trivial\n\ntheorem blocked : True := by sorry\n",
        encoding="utf-8",
    )

    assert (
        declaration_uses_placeholder(tmp_path, "lean/Book/Chapter01.lean", "Book.solved") is False
    )
    assert declaration_uses_placeholder(tmp_path, "lean/Book/Chapter01.lean", "blocked") is True
    assert declaration_uses_placeholder(tmp_path, "lean/Book/Chapter01.lean", "missing") is None
    assert declaration_uses_placeholder_in_chapter(tmp_path, chapter, "Book.solved") is False
    assert declaration_uses_placeholder_in_chapter(tmp_path, chapter, "blocked") is True
    assert declaration_uses_placeholder_in_chapter(tmp_path, chapter, "missing") is None


def test_warning_filter_allows_only_declaration_uses_sorry() -> None:
    output = """warning: Book/Chapter.lean:12:8: declaration uses `sorry`
warning: Book/Chapter.lean:18:5: Variable name `h` is not explicitly referenced.
warning: declaration uses 'sorry'
warning: Book/Chapter.lean:24:2: a warning that merely mentions sorry
"""

    assert unexpected_lean_warnings(output) == (
        "warning: Book/Chapter.lean:18:5: Variable name `h` is not explicitly referenced.",
        "warning: Book/Chapter.lean:24:2: a warning that merely mentions sorry",
    )


def test_bounded_feedback_preserves_endpoints_and_indexes_omitted_diagnostics() -> None:
    feedback = (
        "FIRST DIAGNOSTIC\n"
        + ("middle\n" * 4000)
        + "error: Book/Chapter.lean:42:7: hidden failure\n"
        + ("more middle\n" * 4000)
        + "LAST DIAGNOSTIC"
    )

    bounded = _bounded_feedback(feedback)

    assert len(bounded) == 48000
    assert bounded.startswith("FIRST DIAGNOSTIC")
    assert bounded.endswith("LAST DIAGNOSTIC")
    assert "coordinator feedback body omitted" in bounded
    assert "Book/Chapter.lean:42:7: hidden failure" in bounded


@pytest.mark.asyncio
async def test_validation_rejects_non_sorry_warnings(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = replace(
        config.chapters[0],
        build_command=(
            "printf '%s\\n' 'warning: Book/Chapter.lean:1:1: declaration uses `sorry`' "
            "'warning: Book/Chapter.lean:2:1: unused variable'"
        ),
    )

    validation = await validate(config, chapter)

    assert not validation.succeeded
    assert validation.exit_code == 1
    assert "Coordinator rejected 1 non-sorry Lean warning(s)" in validation.output
    assert "unused variable" in validation.output


@pytest.mark.asyncio
async def test_validation_accepts_sorry_warnings(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = replace(
        config.chapters[0],
        build_command=("printf '%s\\n' 'warning: Book/Chapter.lean:1:1: declaration uses `sorry`'"),
    )

    validation = await validate(config, chapter)

    assert validation.succeeded
    assert validation.exit_code == 0


@pytest.mark.asyncio
async def test_validation_streams_build_output(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = replace(
        config.chapters[0],
        build_command="printf '%s\\n' 'building dependency' 'compiling chapter'",
    )
    output: list[str] = []

    validation = await validate(config, chapter, on_output=output.append)

    assert validation.succeeded
    assert output == ["building dependency\n", "compiling chapter\n"]


@pytest.mark.asyncio
async def test_validation_retains_structured_evidence_before_bounded_output(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    chapter = replace(
        config.chapters[0],
        build_command=(
            "printf '%s\\n' 'error: Book/Chapter.lean:7:3: early failure'; "
            "yes 'warning: Book/Other.lean:1:1: unused variable' | head -n 1000; "
            "printf '%s\\n' 'Some required targets logged failures:' '- Book.Chapter'; "
            "exit 1"
        ),
    )

    validation = await validate(config, chapter)

    assert validation.process_exit_code == 1
    assert len(validation.output) == 20_000
    assert "coordinator build output omitted" in validation.output
    assert next(diagnostic.header for diagnostic in validation.diagnostics) == (
        "error: Book/Chapter.lean:7:3: early failure"
    )
    assert validation.failed_modules == ("Book.Chapter",)
    assert validation.raw_log_path is not None
    assert validation.as_dict()["diagnostics"][0]["severity"] == "error"
    assert validation.as_dict()["failed_modules"] == ["Book.Chapter"]
    raw_log = Path(validation.raw_log_path)
    assert raw_log.is_file()
    assert "early failure" in raw_log.read_text(encoding="utf-8")


@pytest.mark.asyncio
async def test_executor_consumes_jsonl_report_and_usage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    event_times = iter(
        [
            "2026-08-14T10:00:00+00:00",
            "2026-08-14T10:00:01+00:00",
            "2026-08-14T10:00:02+00:00",
        ]
    )
    monkeypatch.setattr(codex_module, "activity_timestamp", lambda: next(event_times))
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    fake_codex = tmp_path / "fake-codex"
    fake_codex.write_text(
        """#!/usr/bin/env python3
import json
import sys

sys.stdin.read()
print(json.dumps({"type": "thread.started", "thread_id": "thread-123"}))
print(json.dumps({"type": "turn.completed", "usage": {
    "input_tokens": 100, "cached_input_tokens": 80, "output_tokens": 25,
    "reasoning_output_tokens": 10}}))
report = {"complete": True,
          "summary": "done", "issues": []}
print(json.dumps({"type": "item.completed", "item": {
    "type": "agent_message", "text": json.dumps(report)}}))
""",
        encoding="utf-8",
    )
    fake_codex.chmod(0o755)
    config = replace(config, settings=replace(config.settings, codex_bin=str(fake_codex)))
    state = StateStore(config)
    await state.load_or_create()
    executor = CodexExecutor(config, state)
    await executor.prepare()
    run = await state.start_run(config.chapters[0].id, Stage.REVIEW)
    expected_prompt = executor.build_prompt(config.chapters[0], Stage.REVIEW)

    result = await executor.run(config.chapters[0], Stage.REVIEW, run)

    assert result.succeeded
    assert result.thread_id == "thread-123"
    assert result.usage.total_tokens == 125
    assert result.report["summary"] == "done"
    prompt_path = state.logs_dir / f"{run.id}.prompt.md"
    assert prompt_path.read_text(encoding="utf-8") == expected_prompt
    activity = state.activities.get(run.id)
    assert activity is not None
    assert activity.current == "agent succeeded"
    assert json.loads(activity.latest_summary)["summary"] == "done"
    log_path = state.logs_dir / f"{run.id}.jsonl"
    logged_events = [json.loads(line) for line in log_path.read_text().splitlines()]
    recorded_at = [event[EVENT_TIMESTAMP_FIELD] for event in logged_events]
    assert recorded_at == [
        "2026-08-14T10:00:00+00:00",
        "2026-08-14T10:00:01+00:00",
        "2026-08-14T10:00:02+00:00",
    ]
    replayed = state.activities.replay(
        run.id,
        run.chapter_id,
        Stage.REVIEW.value,
        log_path,
        workspace_root=tmp_path,
        cache=False,
    )
    assert replayed is not None
    assert [entry.at for entry in replayed.recent] == recorded_at


@pytest.mark.asyncio
async def test_executor_resumes_same_thread_after_capacity_failure(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    invocations_path = tmp_path / "invocations.jsonl"
    fake_codex = tmp_path / "capacity-codex"
    fake_codex.write_text(
        f"""#!/usr/bin/env python3
import json
import sys
from pathlib import Path

prompt = sys.stdin.read()
record = {{"args": sys.argv[1:], "prompt": prompt}}
with Path({str(invocations_path)!r}).open("a", encoding="utf-8") as handle:
    handle.write(json.dumps(record) + "\\n")
if "resume" not in sys.argv:
    print(json.dumps({{"type": "thread.started", "thread_id": "capacity-thread"}}))
    message = "Error running remote compact task: Selected model is at capacity."
    print(json.dumps({{"type": "error", "message": message}}))
    print(json.dumps({{"type": "turn.failed", "error": {{"message": message}}}}))
    raise SystemExit(1)
report = {{"complete": True,
          "summary": "resumed successfully", "issues": []}}
print(json.dumps({{"type": "turn.started"}}))
print(json.dumps({{"type": "item.completed", "item": {{
    "type": "agent_message", "text": json.dumps(report)}}}}))
""",
        encoding="utf-8",
    )
    fake_codex.chmod(0o755)
    config = replace(
        config,
        settings=replace(
            config.settings,
            codex_bin=str(fake_codex),
            capacity_resume_attempts=2,
            capacity_resume_delay_seconds=0,
        ),
    )
    state = StateStore(config)
    await state.load_or_create()
    executor = CodexExecutor(config, state)
    await executor.prepare()
    run = await state.start_run(config.chapters[0].id, Stage.FORMALIZE)

    result = await executor.run(config.chapters[0], Stage.FORMALIZE, run)

    invocations = [json.loads(line) for line in invocations_path.read_text().splitlines()]
    assert result.succeeded
    assert result.thread_id == "capacity-thread"
    assert result.report["summary"] == "resumed successfully"
    assert len(invocations) == 2
    assert invocations[1]["args"][:2] == ["exec", "resume"]
    assert "capacity-thread" in invocations[1]["args"]
    log = (state.logs_dir / f"{run.id}.jsonl").read_text(encoding="utf-8")
    assert "remote compact task" in log
    assert "resumed successfully" in log
    activity = state.activities.get(run.id)
    assert activity is not None
    assert activity.current == "agent succeeded"
    assert any(entry.status == "retrying" for entry in activity.recent)


@pytest.mark.asyncio
async def test_executor_aborts_invalid_output_schema_with_visible_error(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    fake_codex = tmp_path / "invalid-schema-codex"
    fake_codex.write_text(
        """#!/usr/bin/env python3
import json
import sys

sys.stdin.read()
print(json.dumps({"type": "thread.started", "thread_id": "schema-thread"}))
message = json.dumps({
    "type": "error",
    "error": {
        "type": "invalid_request_error",
        "code": "invalid_json_schema",
        "message": "Invalid schema: 'uniqueItems' is not permitted.",
    },
    "status": 400,
})
print(json.dumps({"type": "error", "message": message}))
print(json.dumps({"type": "turn.failed", "error": {"message": message}}))
raise SystemExit(1)
""",
        encoding="utf-8",
    )
    fake_codex.chmod(0o755)
    config = replace(config, settings=replace(config.settings, codex_bin=str(fake_codex)))
    state = StateStore(config)
    await state.load_or_create()
    executor = CodexExecutor(config, state)
    await executor.prepare()
    run = await state.start_run(config.chapters[0].id, Stage.FORMALIZE)

    with pytest.raises(FatalCodexInvocationError, match=r"uniqueItems.*not permitted"):
        await executor.run(config.chapters[0], Stage.FORMALIZE, run)

    activity = state.activities.get(run.id)
    assert activity is not None
    assert activity.current == "agent failed"
    assert activity.latest_error == "Invalid schema: 'uniqueItems' is not permitted."


@pytest.mark.asyncio
async def test_capacity_resume_uses_the_original_attempt_deadline(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    invocations_path = tmp_path / "deadline-invocations"
    fake_codex = tmp_path / "deadline-codex"
    fake_codex.write_text(
        f"""#!/usr/bin/env python3
import json
import sys
import time
from pathlib import Path

sys.stdin.read()
with Path({str(invocations_path)!r}).open("a", encoding="utf-8") as handle:
    handle.write("run\\n")
if "resume" not in sys.argv:
    print(json.dumps({{"type": "thread.started", "thread_id": "deadline-thread"}}))
    message = "Selected model is at capacity."
    print(json.dumps({{"type": "turn.failed", "error": {{"message": message}}}}))
    time.sleep(0.15)
    raise SystemExit(1)
time.sleep(60)
""",
        encoding="utf-8",
    )
    fake_codex.chmod(0o755)
    config = replace(
        config,
        settings=replace(
            config.settings,
            codex_bin=str(fake_codex),
            agent_timeout_seconds=0.3,
            capacity_resume_attempts=2,
            capacity_resume_delay_seconds=0,
        ),
    )
    state = StateStore(config)
    await state.load_or_create()
    executor = CodexExecutor(config, state)
    await executor.prepare()
    run = await state.start_run(config.chapters[0].id, Stage.REVIEW)
    started = asyncio.get_running_loop().time()

    result = await executor.run(config.chapters[0], Stage.REVIEW, run)
    elapsed = asyncio.get_running_loop().time() - started

    assert result.exit_code == 124
    assert result.error == "agent timed out"
    assert invocations_path.read_text(encoding="utf-8").splitlines() == ["run", "run"]
    assert elapsed < 0.4


@pytest.mark.asyncio
async def test_executor_recycles_fd_leaking_codex_and_resumes_same_thread(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    invocations_path = tmp_path / "fd-invocations.jsonl"
    fake_codex = tmp_path / "fd-leaking-codex"
    fake_codex.write_text(
        f"""#!/usr/bin/env python3
import json
import sys
import time
from pathlib import Path

prompt = sys.stdin.read()
with Path({str(invocations_path)!r}).open("a", encoding="utf-8") as handle:
    handle.write(json.dumps({{"args": sys.argv[1:], "prompt": prompt}}) + "\\n")
if "resume" not in sys.argv:
    print(json.dumps({{"type": "thread.started", "thread_id": "fd-thread"}}), flush=True)
    time.sleep(60)
report = {{"complete": True,
          "summary": "resumed after fd recycle", "issues": []}}
print(json.dumps({{"type": "item.completed", "item": {{
    "type": "agent_message", "text": json.dumps(report)}}}}))
""",
        encoding="utf-8",
    )
    fake_codex.chmod(0o755)
    config = replace(
        config,
        settings=replace(
            config.settings,
            codex_bin=str(fake_codex),
            codex_fd_recycle_threshold=256,
            codex_fd_recycle_attempts=2,
        ),
    )
    monitor_calls = 0

    async def fd_pressure(
        process: asyncio.subprocess.Process,
        _process_tree: object,
        _threshold: int,
        resumable: Callable[[], bool],
    ) -> int:
        nonlocal monitor_calls
        monitor_calls += 1
        if monitor_calls == 1:
            while not resumable():
                await asyncio.sleep(0.01)
            return 300
        await process.wait()
        return 0

    monkeypatch.setattr(codex_module, "_wait_for_fd_pressure", fd_pressure)
    state = StateStore(config)
    await state.load_or_create()
    executor = CodexExecutor(config, state)
    await executor.prepare()
    run = await state.start_run(config.chapters[0].id, Stage.REVIEW)

    result = await executor.run(config.chapters[0], Stage.REVIEW, run)

    invocations = [json.loads(line) for line in invocations_path.read_text().splitlines()]
    assert result.succeeded
    assert result.thread_id == "fd-thread"
    assert len(invocations) == 2
    assert invocations[1]["args"][:2] == ["exec", "resume"]
    activity = state.activities.get(run.id)
    assert activity is not None
    assert any("resource recycle" in entry.title for entry in activity.recent)


@pytest.mark.asyncio
async def test_fd_pressure_and_teardown_include_setsid_descendants(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(codex_module, "PROCESS_GROUP_GRACE_SECONDS", 0.02)
    child_pid_path = tmp_path / "setsid-child.pid"
    child = tmp_path / "descriptor-child"
    child.write_text(
        """#!/usr/bin/env python3
import os
import signal
import sys
import time
from pathlib import Path

signal.signal(signal.SIGTERM, signal.SIG_IGN)
descriptors = [open("/dev/null", "rb") for _ in range(80)]
Path(sys.argv[1]).write_text(str(os.getpid()))
time.sleep(60)
""",
        encoding="utf-8",
    )
    child.chmod(0o755)
    parent = tmp_path / "descriptor-parent"
    parent.write_text(
        f"""#!/usr/bin/env python3
import subprocess
import time

subprocess.Popen([{str(child)!r}, {str(child_pid_path)!r}], start_new_session=True)
time.sleep(60)
""",
        encoding="utf-8",
    )
    parent.chmod(0o755)
    process = await asyncio.create_subprocess_exec(
        str(parent),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
        start_new_session=True,
    )
    process_tree = codex_module._ProcessTreeTracker(process.pid)
    child_pid = 0
    try:
        for _ in range(200):
            process_tree.scan()
            if child_pid_path.is_file():
                child_pid = int(child_pid_path.read_text(encoding="utf-8"))
                break
            await asyncio.sleep(0.01)
        assert child_pid
        pressure = await asyncio.wait_for(
            codex_module._wait_for_fd_pressure(process, process_tree, 64, lambda: True),
            timeout=5,
        )
        assert pressure >= 64
    finally:
        await codex_module._terminate(process, process_tree)
    assert not _process_is_running(child_pid)


@pytest.mark.asyncio
async def test_executor_consumes_jsonl_records_larger_than_stream_limit(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    fake_codex = tmp_path / "large-event-codex"
    fake_codex.write_text(
        """#!/usr/bin/env python3
import json
import sys

sys.stdin.read()
print(json.dumps({"type": "item.completed", "item": {
    "type": "command_execution", "aggregated_output": "x" * (2 * 1024 * 1024)}}))
print(json.dumps({"type": "turn.completed", "usage": {
    "input_tokens": 100, "cached_input_tokens": 80, "output_tokens": 25,
    "reasoning_output_tokens": 10}}))
report = {"complete": True,
          "summary": "large event drained", "issues": []}
print(json.dumps({"type": "item.completed", "item": {
    "type": "agent_message", "text": json.dumps(report)}}))
""",
        encoding="utf-8",
    )
    fake_codex.chmod(0o755)
    config = replace(config, settings=replace(config.settings, codex_bin=str(fake_codex)))
    state = StateStore(config)
    await state.load_or_create()
    executor = CodexExecutor(config, state)
    await executor.prepare()
    run = await state.start_run(config.chapters[0].id, Stage.REVIEW)

    result = await executor.run(config.chapters[0], Stage.REVIEW, run)

    assert result.succeeded
    assert result.usage.total_tokens == 125
    assert result.report["summary"] == "large event drained"
    assert (state.logs_dir / f"{run.id}.jsonl").stat().st_size > 2 * 1024 * 1024


@pytest.mark.asyncio
async def test_executor_flushes_jsonl_while_agent_is_running(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    fake_codex = tmp_path / "slow-codex"
    codex_home = tmp_path / "codex-home"
    fake_codex.write_text(
        """#!/usr/bin/env python3
import json
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

sys.stdin.read()
thread_id = "visible-now"
session = Path(os.environ["CODEX_HOME"]) / "sessions" / datetime.now(UTC).strftime("%Y/%m/%d")
session.mkdir(parents=True)
rollout = session / f"rollout-test-{thread_id}.jsonl"
rollout.write_text(json.dumps({"type": "event_msg", "payload": {
    "type": "token_count", "info": {"total_token_usage": {
        "input_tokens": 100, "cached_input_tokens": 80, "output_tokens": 25,
        "reasoning_output_tokens": 10, "total_tokens": 125}}}}) + "\\n")
print(json.dumps({"type": "thread.started", "thread_id": thread_id}), flush=True)
time.sleep(60)
""",
        encoding="utf-8",
    )
    fake_codex.chmod(0o755)
    monkeypatch.setenv("CODEX_HOME", str(codex_home))
    config = replace(config, settings=replace(config.settings, codex_bin=str(fake_codex)))
    state = StateStore(config)
    await state.load_or_create()
    executor = CodexExecutor(config, state)
    await executor.prepare()
    run = await state.start_run(config.chapters[0].id, Stage.REVIEW)
    task = asyncio.create_task(executor.run(config.chapters[0], Stage.REVIEW, run))
    log_path = state.logs_dir / f"{run.id}.jsonl"
    try:
        for _ in range(100):
            if log_path.is_file() and log_path.stat().st_size and run.usage.measured:
                break
            await asyncio.sleep(0.01)
        assert "visible-now" in log_path.read_text(encoding="utf-8")
        assert run.usage.total_tokens == 125
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    activity = state.activities.get(run.id)
    assert activity is not None
    assert activity.current == "agent cancelled"
    assert run.status == TaskStatus.INTERRUPTED
    assert run.thread_id == "visible-now"
    assert state.task(run.chapter_id, Stage.REVIEW).status == TaskStatus.INTERRUPTED
    reloaded = read_full_snapshot(config.settings.state_dir)
    assert reloaded is not None
    persisted = reloaded["tasks"][f"{run.chapter_id}:review"]["runs"][-1]["usage"]
    assert persisted["input_tokens"] == 100
    assert persisted["output_tokens"] == 25


@pytest.mark.asyncio
async def test_executor_resumes_interrupted_session(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    invocations_path = tmp_path / "resume-invocations.jsonl"
    fake_codex = tmp_path / "resume-codex"
    fake_codex.write_text(
        f"""#!/usr/bin/env python3
import json
import sys
from pathlib import Path

prompt = sys.stdin.read()
with Path({str(invocations_path)!r}).open("a", encoding="utf-8") as handle:
    handle.write(json.dumps({{"args": sys.argv[1:], "prompt": prompt}}) + "\\n")
report = {{"complete": True,
          "summary": "resumed interrupted work", "issues": []}}
print(json.dumps({{"type": "thread.started", "thread_id": "saved-session"}}))
print(json.dumps({{"type": "item.completed", "item": {{
    "type": "agent_message", "text": json.dumps(report)}}}}))
""",
        encoding="utf-8",
    )
    fake_codex.chmod(0o755)
    config = replace(config, settings=replace(config.settings, codex_bin=str(fake_codex)))
    state = StateStore(config)
    await state.load_or_create()
    interrupted = await state.start_run(config.chapters[0].id, Stage.REVIEW)
    await state.finish_run(
        interrupted,
        status=TaskStatus.INTERRUPTED,
        thread_id="saved-session",
    )
    await state.requeue_interrupted(resume_agents=True)
    run = await state.start_run(config.chapters[0].id, Stage.REVIEW)
    executor = CodexExecutor(config, state, resume_agents=True)
    await executor.prepare()

    result = await executor.run(config.chapters[0], Stage.REVIEW, run)

    invocation = json.loads(invocations_path.read_text(encoding="utf-8"))
    assert invocation["args"][:2] == ["exec", "resume"]
    assert "saved-session" in invocation["args"]
    assert result.succeeded
    assert result.thread_id == "saved-session"
    assert run.resumed_from_run_id == interrupted.id


@pytest.mark.asyncio
async def test_executor_only_resumes_the_matching_immediate_predecessor(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    await state.load_or_create()
    interrupted = await state.start_run(config.chapters[0].id, Stage.REVIEW)
    await state.finish_run(
        interrupted,
        status=TaskStatus.INTERRUPTED,
        thread_id="saved-session",
    )
    executor = CodexExecutor(config, state, resume_agents=True)
    await executor.prepare()

    resumed = await state.start_run(config.chapters[0].id, Stage.REVIEW)
    assert executor._resumable_run(resumed, Stage.REVIEW) is interrupted

    await state.finish_run(resumed, status=TaskStatus.SUCCEEDED)
    fresh = await state.start_run(config.chapters[0].id, Stage.REVIEW)
    assert executor._resumable_run(fresh, Stage.REVIEW) is None

    await state.finish_run(fresh, status=TaskStatus.INTERRUPTED, thread_id="other-session")
    mismatched = await state.start_run(config.chapters[0].id, Stage.REVIEW)
    await state.update_run(fresh, prompt_kind="review")
    await state.update_run(mismatched, prompt_kind="proof_review")
    assert executor._resumable_run(mismatched, Stage.REVIEW) is None


@pytest.mark.asyncio
async def test_interrupted_predecessor_restores_overlay_even_without_a_thread_id(
    tmp_path: Path,
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    state = StateStore(config)
    await state.load_or_create()
    interrupted = await state.start_run(config.chapters[0].id, Stage.FORMALIZE)
    await state.finish_run(interrupted, status=TaskStatus.INTERRUPTED)
    resumed = await state.start_run(config.chapters[0].id, Stage.FORMALIZE)
    executor = CodexExecutor(config, state, resume_agents=True)

    assert executor.interrupted_predecessor(resumed, Stage.FORMALIZE) is interrupted
    assert executor.resumable_run(resumed, Stage.FORMALIZE) is None


@pytest.mark.asyncio
async def test_executor_starts_new_agent_when_session_cannot_resume(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    invocations_path = tmp_path / "fallback-invocations.jsonl"
    fake_codex = tmp_path / "fallback-codex"
    fake_codex.write_text(
        f"""#!/usr/bin/env python3
import json
import sys
from pathlib import Path

prompt = sys.stdin.read()
with Path({str(invocations_path)!r}).open("a", encoding="utf-8") as handle:
    handle.write(json.dumps({{"args": sys.argv[1:], "prompt": prompt}}) + "\\n")
if "resume" in sys.argv:
    print(json.dumps({{"type": "error", "message": "session not found"}}))
    raise SystemExit(1)
report = {{"complete": True,
          "summary": "started replacement agent", "issues": []}}
print(json.dumps({{"type": "thread.started", "thread_id": "replacement-session"}}))
print(json.dumps({{"type": "item.completed", "item": {{
    "type": "agent_message", "text": json.dumps(report)}}}}))
""",
        encoding="utf-8",
    )
    fake_codex.chmod(0o755)
    config = replace(config, settings=replace(config.settings, codex_bin=str(fake_codex)))
    state = StateStore(config)
    await state.load_or_create()
    interrupted = await state.start_run(config.chapters[0].id, Stage.REVIEW)
    await state.finish_run(
        interrupted,
        status=TaskStatus.INTERRUPTED,
        thread_id="missing-session",
    )
    await state.requeue_interrupted(resume_agents=True)
    run = await state.start_run(config.chapters[0].id, Stage.REVIEW)
    executor = CodexExecutor(config, state, resume_agents=True)
    await executor.prepare()

    result = await executor.run(config.chapters[0], Stage.REVIEW, run)

    invocations = [json.loads(line) for line in invocations_path.read_text().splitlines()]
    assert len(invocations) == 2
    assert invocations[0]["args"][:2] == ["exec", "resume"]
    assert invocations[1]["args"][0] == "exec"
    assert "resume" not in invocations[1]["args"]
    assert result.succeeded
    assert result.thread_id == "replacement-session"


@pytest.mark.asyncio
async def test_cancellation_kills_surviving_mcp_descendants(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(codex_module, "PROCESS_GROUP_GRACE_SECONDS", 0.02)
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    child_pid_path = tmp_path / "child.pid"
    child = tmp_path / "term-resistant-child"
    child.write_text(
        """#!/usr/bin/env python3
import os
import signal
import sys
import time
from pathlib import Path

signal.signal(signal.SIGTERM, signal.SIG_IGN)
Path(sys.argv[1]).write_text(str(os.getpid()))
time.sleep(60)
""",
        encoding="utf-8",
    )
    child.chmod(0o755)
    fake_codex = tmp_path / "codex-with-mcp-child"
    fake_codex.write_text(
        f"""#!/usr/bin/env python3
import json
import subprocess
import sys
import time

sys.stdin.read()
subprocess.Popen([{str(child)!r}, {str(child_pid_path)!r}], start_new_session=True)
print(json.dumps({{"type": "thread.started", "thread_id": "descendant-test"}}), flush=True)
time.sleep(60)
""",
        encoding="utf-8",
    )
    fake_codex.chmod(0o755)
    config = replace(config, settings=replace(config.settings, codex_bin=str(fake_codex)))
    state = StateStore(config)
    await state.load_or_create()
    executor = CodexExecutor(config, state)
    await executor.prepare()
    run = await state.start_run(config.chapters[0].id, Stage.REVIEW)
    task = asyncio.create_task(executor.run(config.chapters[0], Stage.REVIEW, run))

    child_pid = 0
    try:
        for _ in range(200):
            if child_pid_path.is_file():
                child_pid = int(child_pid_path.read_text(encoding="utf-8"))
                break
            await asyncio.sleep(0.01)
        assert child_pid
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not _process_is_running(child_pid)
    finally:
        task.cancel()
        if child_pid and _process_is_running(child_pid):
            os.kill(child_pid, signal.SIGKILL)


@pytest.mark.skip(reason="slow TERM-resistant descendant cleanup integration test")
@pytest.mark.asyncio
async def test_successful_agent_exit_kills_surviving_mcp_descendants(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    child_pid_path = tmp_path / "successful-child.pid"
    child = tmp_path / "successful-term-resistant-child"
    child.write_text(
        """#!/usr/bin/env python3
import os
import signal
import sys
import time
from pathlib import Path

signal.signal(signal.SIGTERM, signal.SIG_IGN)
Path(sys.argv[1]).write_text(str(os.getpid()))
time.sleep(60)
""",
        encoding="utf-8",
    )
    child.chmod(0o755)
    fake_codex = tmp_path / "successful-codex-with-mcp-child"
    fake_codex.write_text(
        f"""#!/usr/bin/env python3
import json
import subprocess
import sys
import time
from pathlib import Path

sys.stdin.read()
subprocess.Popen([{str(child)!r}, {str(child_pid_path)!r}], start_new_session=True)
for _ in range(200):
    if Path({str(child_pid_path)!r}).is_file():
        break
    time.sleep(0.01)
report = {{"complete": True,
          "summary": "done", "issues": []}}
print(json.dumps({{"type": "item.completed", "item": {{
    "type": "agent_message", "text": json.dumps(report)}}}}), flush=True)
""",
        encoding="utf-8",
    )
    fake_codex.chmod(0o755)
    config = replace(config, settings=replace(config.settings, codex_bin=str(fake_codex)))
    state = StateStore(config)
    await state.load_or_create()
    executor = CodexExecutor(config, state)
    await executor.prepare()
    run = await state.start_run(config.chapters[0].id, Stage.REVIEW)

    child_pid = 0
    try:
        result = await executor.run(config.chapters[0], Stage.REVIEW, run)
        child_pid = int(child_pid_path.read_text(encoding="utf-8"))
        assert result.succeeded
        assert not _process_is_running(child_pid)
    finally:
        if child_pid and _process_is_running(child_pid):
            os.kill(child_pid, signal.SIGKILL)


@pytest.mark.asyncio
async def test_cancellation_while_recording_spawned_pid_reaps_agent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    pid_path = tmp_path / "cancelled-before-pid-save.pid"
    fake_codex = tmp_path / "codex-cancelled-before-pid-save"
    fake_codex.write_text(
        f"""#!/usr/bin/env python3
import os
import sys
import time
from pathlib import Path

Path({str(pid_path)!r}).write_text(str(os.getpid()))
sys.stdin.read()
time.sleep(60)
""",
        encoding="utf-8",
    )
    fake_codex.chmod(0o755)
    config = replace(config, settings=replace(config.settings, codex_bin=str(fake_codex)))
    state = StateStore(config)
    await state.load_or_create()
    executor = CodexExecutor(config, state)
    await executor.prepare()
    run = await state.start_run(config.chapters[0].id, Stage.REVIEW)
    original_update_run = state.update_run
    pid_update_started = asyncio.Event()
    never = asyncio.Event()

    async def block_pid_update(target_run, **changes):
        if "pid" in changes:
            pid_update_started.set()
            await never.wait()
        await original_update_run(target_run, **changes)

    monkeypatch.setattr(state, "update_run", block_pid_update)
    task = asyncio.create_task(executor.run(config.chapters[0], Stage.REVIEW, run))

    pid = 0
    try:
        await asyncio.wait_for(pid_update_started.wait(), timeout=5)
        for _ in range(200):
            if pid_path.is_file():
                pid = int(pid_path.read_text(encoding="utf-8"))
                break
            await asyncio.sleep(0.01)
        assert pid
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        for _ in range(200):
            if not _process_is_running(pid):
                break
            await asyncio.sleep(0.01)
        assert not _process_is_running(pid)
    finally:
        task.cancel()
        if pid and _process_is_running(pid):
            os.kill(pid, signal.SIGKILL)


def _process_is_running(pid: int) -> bool:
    try:
        state = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8").split()[2]
    except (FileNotFoundError, IndexError):
        return False
    return state != "Z"
