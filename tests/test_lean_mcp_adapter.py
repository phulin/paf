from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from leanclient.aio import LeanClientError

from paf.lean_mcp import (
    _bounded_tool_value,
    _force_prepare_dependencies,
    _hover_columns,
    barrier_with_dependency_refresh,
    compact_target_diagnostics,
    local_search_with_dependencies,
    normalize_lean_project_root,
    prepare_dependencies,
    record_stale_dependency,
    reload_with_dependencies_when_stale,
    resolve_attempt_line,
    resolve_lean_input_path,
)


def test_compact_diagnostics_returns_errors_and_warning_count() -> None:
    diagnostics = [
        {
            "severity": 1,
            "message": "unknown identifier",
            "range": {"start": {"line": 4, "character": 2}, "end": {}},
        },
        {
            "severity": 2,
            "message": "declaration uses sorry",
            "range": {"start": {"line": 8, "character": 0}, "end": {}},
        },
    ]

    result = compact_target_diagnostics(diagnostics, build_success=False)

    assert [(item.severity, item.message) for item in result.items] == [
        ("error", "unknown identifier"),
        ("info", "1 target-file warning(s) suppressed"),
    ]


def test_compact_diagnostics_preserves_dependency_cause_at_any_position() -> None:
    message = "lake setup-file A.lean failed\nerror: B.lean:4:2: unknown identifier 'x'"

    result = compact_target_diagnostics(
        [
            {
                "severity": 1,
                "message": message,
                "range": {
                    "start": {"line": 8, "character": 3},
                    "end": {"line": 8, "character": 4},
                },
            }
        ],
        build_success=False,
    )

    assert result.failed_dependencies == ["B.lean"]


def test_bounded_tool_value_caps_nested_text() -> None:
    result = _bounded_tool_value({"content": "x" * 20_000}, [12 * 1024])

    assert len(result["content"]) <= 12 * 1024
    assert result["content"].endswith("… [truncated]")


@dataclass
class FakeDocument:
    path: str
    uri: str
    text: str
    stale_imports: bool = False
    diagnostics: list[dict[str, Any]] = field(default_factory=list)
    virtual: bool = False


class FakeClient:
    def __init__(self, root: Path) -> None:
        self.project_path = str(root)
        self._docs: dict[str, FakeDocument] = {}
        self._docs_by_uri: dict[str, FakeDocument] = {}
        self.events: list[tuple[Any, ...]] = []
        self.open_modes: dict[str, str] = {}

    async def open(self, path: str, *, wait: bool, dependency_build_mode: str) -> FakeDocument:
        self.events.append(("open", path, wait, dependency_build_mode))
        document = FakeDocument(
            path=path,
            uri=f"file://{self.project_path}/{path}",
            text=(Path(self.project_path) / path).read_text(encoding="utf-8"),
        )
        self._docs[path] = document
        self._docs_by_uri[document.uri] = document
        self.open_modes[path] = dependency_build_mode
        return document

    async def close_file(self, path: str) -> None:
        self.events.append(("close", path))
        document = self._docs.pop(path, None)
        if document is not None:
            self._docs_by_uri.pop(document.uri, None)


def write(root: Path, path: str, text: str) -> None:
    destination = root / path
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(text, encoding="utf-8")


def write_lean_project(root: Path) -> None:
    write(root, "lean-toolchain", "leanprover/lean4:v4.33.0-rc2\n")
    write(root, "lakefile.toml", "name = 'test'\n")


def test_normalizes_paf_merged_workspace_to_nested_lean_root(tmp_path: Path) -> None:
    write_lean_project(tmp_path / "merged" / "lean")

    assert (
        normalize_lean_project_root(tmp_path / "merged") == (tmp_path / "merged" / "lean").resolve()
    )


def test_resolves_module_style_mathlib_path(tmp_path: Path) -> None:
    root = tmp_path / "merged" / "lean"
    write_lean_project(root)
    write(root, ".lake/packages/mathlib/Mathlib/CategoryTheory/Foo.lean", "def foo := 1\n")
    ctx: Any = SimpleNamespace(
        request_context=SimpleNamespace(lifespan_context=SimpleNamespace(lean_project_path=root))
    )

    resolved = resolve_lean_input_path(ctx, "Mathlib/CategoryTheory/Foo.lean")

    assert resolved == (root / ".lake/packages/mathlib/Mathlib/CategoryTheory/Foo.lean").resolve()


def test_local_search_follows_shared_dependency_source_roots(tmp_path: Path) -> None:
    root = tmp_path / "lean"
    packages = tmp_path / "shared-packages"
    write_lean_project(root)
    write(
        packages,
        "mathlib/Mathlib/Dependency/SearchNeedle.lean",
        "namespace Mathlib.Dependency\n\n"
        "theorem searchNeedle : True := by trivial\n\n"
        "end Mathlib.Dependency\n",
    )
    (root / ".lake").mkdir()
    (root / ".lake" / "packages").symlink_to(packages, target_is_directory=True)

    result = local_search_with_dependencies("searchNeedle", limit=5, project_root=root)

    assert result == [
        {
            "name": "Mathlib.Dependency.searchNeedle",
            "kind": "theorem",
            "file": ".lake/packages/mathlib/Mathlib/Dependency/SearchNeedle.lean",
        }
    ]


def test_hover_columns_skip_tactic_keyword_and_snap_to_identifier() -> None:
    line = "  exact CategoryTheory.Functor.map_id F X"

    columns = _hover_columns(line, 3)

    assert columns[0] == line.index("CategoryTheory") + 1
    assert line[columns[0] - 1 :].startswith("CategoryTheory.Functor.map_id")


def test_multi_attempt_resolves_declaration_and_placeholder_anchors() -> None:
    source = """theorem first : True := by
  sorry

theorem second : True := by
  exact True.intro
"""

    assert resolve_attempt_line(source, None, "first", None) == 2
    assert resolve_attempt_line(source, None, None, "exact True.intro") == 5


@pytest.mark.asyncio
async def test_first_open_is_lazy_and_body_edits_preserve_the_worker(tmp_path: Path) -> None:
    write(tmp_path, "A.lean", "import Mathlib\n\ndef a := 1\n")
    client = FakeClient(tmp_path)

    async def original(_client: Any, path: str, wait: bool) -> FakeDocument:
        client.events.append(("reload", path, wait))
        document = client._docs[path]
        document.text = (tmp_path / path).read_text(encoding="utf-8")
        return document

    await reload_with_dependencies_when_stale(client, "A.lean", original=original)
    write(tmp_path, "A.lean", "import Mathlib\n\ndef a := 2\n")
    await reload_with_dependencies_when_stale(client, "A.lean", original=original)

    assert client.events == [
        ("open", "A.lean", False, "never"),
        ("reload", "A.lean", False),
    ]


@pytest.mark.asyncio
async def test_import_edit_prepares_dependencies_before_returning(tmp_path: Path) -> None:
    write(tmp_path, "A.lean", "import Mathlib\n\ndef a := 1\n")
    client = FakeClient(tmp_path)
    await client.open("A.lean", wait=False, dependency_build_mode="never")
    client.events.clear()
    write(tmp_path, "A.lean", "import Mathlib.Algebra.Group.Basic\n\ndef a := 1\n")

    async def barrier(_client: Any, path: str, timeout: float | None) -> None:
        client.events.append(("barrier", path, timeout))

    await reload_with_dependencies_when_stale(
        client,
        "A.lean",
        original_barrier=barrier,
    )

    assert client.events == [
        ("close", "A.lean"),
        ("open", "A.lean", False, "once"),
        ("barrier", "A.lean", None),
    ]


@pytest.mark.asyncio
async def test_stale_diagnostic_rebuilds_and_retries_once(tmp_path: Path) -> None:
    write(tmp_path, "A.lean", "import Mathlib\n")
    client = FakeClient(tmp_path)
    document = await client.open("A.lean", wait=False, dependency_build_mode="never")
    document.diagnostics = [{"message": "Imports are out of date and must be rebuilt"}]
    client.events.clear()

    async def barrier(_client: Any, path: str, timeout: float | None) -> None:
        client.events.append(("barrier", path, timeout))
        if client.open_modes[path] == "once":
            client._docs[path].diagnostics = []

    await barrier_with_dependency_refresh(client, "A.lean", original=barrier)

    assert client.events == [
        ("barrier", "A.lean", None),
        ("close", "A.lean"),
        ("open", "A.lean", False, "once"),
        ("barrier", "A.lean", None),
    ]
    assert client._docs["A.lean"] is document
    assert document.diagnostics == []
    assert client._docs["A.lean"].diagnostics == []


@pytest.mark.asyncio
async def test_missing_dependency_build_stderr_rebuilds_and_retries_once(
    tmp_path: Path,
) -> None:
    write(tmp_path, "A.lean", "import B\n")
    client = FakeClient(tmp_path)
    document = await client.open("A.lean", wait=False, dependency_build_mode="never")
    document.diagnostics = [
        {
            "message": (
                "lake setup-file A.lean failed\n"
                "error: B.lean:1:0: object file 'B.olean' does not exist"
            )
        }
    ]
    client.events.clear()

    async def barrier(_client: Any, path: str, timeout: float | None) -> None:
        client.events.append(("barrier", path, timeout))
        if client.open_modes[path] == "once":
            client._docs[path].diagnostics = []

    await barrier_with_dependency_refresh(client, "A.lean", original=barrier)

    assert client.events == [
        ("barrier", "A.lean", None),
        ("close", "A.lean"),
        ("open", "A.lean", False, "once"),
        ("barrier", "A.lean", None),
    ]
    assert client._docs["A.lean"] is document
    assert document.diagnostics == []


@pytest.mark.asyncio
async def test_non_artifact_dependency_build_failure_does_not_trigger_build(
    tmp_path: Path,
) -> None:
    write(tmp_path, "A.lean", "import B\n")
    client = FakeClient(tmp_path)
    document = await client.open("A.lean", wait=False, dependency_build_mode="never")
    document.diagnostics = [
        {
            "message": (
                "lake setup-file A.lean failed\n"
                "error: B.lean:3:7: type mismatch in dependency source"
            )
        }
    ]
    client.events.clear()

    async def barrier(_client: Any, path: str, timeout: float | None) -> None:
        client.events.append(("barrier", path, timeout))

    await barrier_with_dependency_refresh(client, "A.lean", original=barrier)

    assert client.events == [("barrier", "A.lean", None)]


@pytest.mark.asyncio
async def test_virtual_scratch_dependency_failure_is_not_reopened_from_disk(
    tmp_path: Path,
) -> None:
    write(tmp_path, "A.lean", "import Missing\n")
    client = FakeClient(tmp_path)
    document = await client.open("A.lean", wait=False, dependency_build_mode="never")
    document.virtual = True
    document.stale_imports = True
    client.events.clear()

    async def barrier(_client: Any, path: str, timeout: float | None) -> None:
        client.events.append(("barrier", path, timeout))

    await barrier_with_dependency_refresh(client, "A.lean", original=barrier)

    assert client.events == [("barrier", "A.lean", None)]


@pytest.mark.asyncio
async def test_failed_stale_recovery_is_hidden_and_not_repeated_without_new_state(
    tmp_path: Path,
) -> None:
    write(tmp_path, "A.lean", "import Mathlib\n")
    client = FakeClient(tmp_path)
    document = await client.open("A.lean", wait=False, dependency_build_mode="never")
    document.diagnostics = [{"message": "Imports are out of date and must be rebuilt"}]
    client.events.clear()

    async def barrier(_client: Any, path: str, timeout: float | None) -> None:
        client.events.append(("barrier", path, timeout))
        client._docs[path].diagnostics = [
            {"message": "Imports are out of date and must be rebuilt"}
        ]

    with pytest.raises(LeanClientError, match="usable dependency snapshot"):
        await barrier_with_dependency_refresh(client, "A.lean", original=barrier)
    with pytest.raises(LeanClientError, match="usable dependency snapshot"):
        await barrier_with_dependency_refresh(client, "A.lean", original=barrier)

    assert [event for event in client.events if event[0] == "open"] == [
        ("open", "A.lean", False, "once")
    ]

    def publish(_client: Any, _method: str, params: dict[str, Any]) -> None:
        client._docs["A.lean"].diagnostics = params["diagnostics"]

    record_stale_dependency(
        client,
        "textDocument/publishDiagnostics",
        {
            "uri": client._docs["A.lean"].uri,
            "diagnostics": [{"message": "Imports are out of date and must be rebuilt"}],
        },
        original=publish,
    )
    with pytest.raises(LeanClientError, match="usable dependency snapshot"):
        await barrier_with_dependency_refresh(client, "A.lean", original=barrier)
    assert [event for event in client.events if event[0] == "open"] == [
        ("open", "A.lean", False, "once"),
        ("open", "A.lean", False, "once"),
    ]


@pytest.mark.asyncio
async def test_new_dependency_build_failure_notification_allows_retry(tmp_path: Path) -> None:
    write(tmp_path, "A.lean", "import B\n")
    client = FakeClient(tmp_path)
    document = await client.open("A.lean", wait=False, dependency_build_mode="never")
    build_failure = {
        "message": (
            "lake setup-file A.lean failed\nerror: B.lean:1:0: object file 'B.olean' does not exist"
        )
    }
    document.diagnostics = [build_failure]
    client.events.clear()

    async def barrier(_client: Any, path: str, timeout: float | None) -> None:
        client.events.append(("barrier", path, timeout))
        client._docs[path].diagnostics = [build_failure]

    with pytest.raises(LeanClientError, match="usable dependency snapshot"):
        await barrier_with_dependency_refresh(client, "A.lean", original=barrier)
    with pytest.raises(LeanClientError, match="usable dependency snapshot"):
        await barrier_with_dependency_refresh(client, "A.lean", original=barrier)

    def publish(_client: Any, _method: str, params: dict[str, Any]) -> None:
        client._docs["A.lean"].diagnostics = params["diagnostics"]

    record_stale_dependency(
        client,
        "textDocument/publishDiagnostics",
        {"uri": client._docs["A.lean"].uri, "diagnostics": [build_failure]},
        original=publish,
    )
    with pytest.raises(LeanClientError, match="usable dependency snapshot"):
        await barrier_with_dependency_refresh(client, "A.lean", original=barrier)

    assert [event for event in client.events if event[0] == "open"] == [
        ("open", "A.lean", False, "once"),
        ("open", "A.lean", False, "once"),
    ]


@pytest.mark.asyncio
async def test_explicit_prepare_retries_after_failed_dependency_source_is_fixed(
    tmp_path: Path,
) -> None:
    write(tmp_path, "A.lean", "import B\n")
    write(tmp_path, "B.lean", "def b : Nat := broken\n")
    client = FakeClient(tmp_path)
    build_failure = "lake setup-file A.lean failed\nerror: B.lean:1:15: unknown identifier 'broken'"

    async def barrier(_client: Any, path: str, timeout: float | None) -> None:
        client.events.append(("barrier", path, timeout))
        if "broken" in (tmp_path / "B.lean").read_text(encoding="utf-8"):
            client._docs[path].diagnostics = [{"message": build_failure}]
        else:
            client._docs[path].diagnostics = []

    _, first_errors = await _force_prepare_dependencies(client, "A.lean", original_barrier=barrier)
    write(tmp_path, "B.lean", "def b : Nat := 0\n")
    _, second_errors = await _force_prepare_dependencies(client, "A.lean", original_barrier=barrier)

    assert first_errors == [
        {
            "file_path": "A.lean",
            "message": build_failure,
            "failed_dependencies": ["B.lean"],
        }
    ]
    assert second_errors == []
    assert [event for event in client.events if event[0] == "open"] == [
        ("open", "A.lean", False, "once"),
        ("open", "A.lean", False, "once"),
    ]


@pytest.mark.asyncio
async def test_explicit_prepare_threads_raw_missing_artifact_error(tmp_path: Path) -> None:
    write(tmp_path, "A.lean", "import B\n")
    client = FakeClient(tmp_path)
    build_failure = (
        "lake setup-file A.lean failed\nerror: B.lean:1:0: object file 'B.olean' does not exist"
    )

    async def barrier(_client: Any, path: str, timeout: float | None) -> None:
        client.events.append(("barrier", path, timeout))
        client._docs[path].diagnostics = [{"message": build_failure}]

    _, errors = await _force_prepare_dependencies(client, "A.lean", original_barrier=barrier)

    assert errors == [
        {
            "file_path": "A.lean",
            "message": build_failure,
            "failed_dependencies": ["B.lean"],
        }
    ]


@pytest.mark.asyncio
async def test_explicit_prepare_allows_dependency_sorry_warnings(tmp_path: Path) -> None:
    write(tmp_path, "A.lean", "import B\n")
    client = FakeClient(tmp_path)
    build_output = "lake setup-file A.lean failed\nwarning: B.lean:2:8: declaration uses 'sorry'"

    async def barrier(_client: Any, path: str, timeout: float | None) -> None:
        client.events.append(("barrier", path, timeout))
        client._docs[path].diagnostics = [{"message": build_output}]

    _, errors = await _force_prepare_dependencies(client, "A.lean", original_barrier=barrier)

    assert errors == []


@pytest.mark.asyncio
async def test_explicit_prepare_rejects_non_sorry_dependency_warnings(tmp_path: Path) -> None:
    write(tmp_path, "A.lean", "import B\n")
    client = FakeClient(tmp_path)
    build_output = "lake setup-file A.lean failed\nwarning: B.lean:2:8: unused variable `value`"

    async def barrier(_client: Any, path: str, timeout: float | None) -> None:
        client.events.append(("barrier", path, timeout))
        client._docs[path].diagnostics = [{"message": build_output}]

    _, errors = await _force_prepare_dependencies(client, "A.lean", original_barrier=barrier)

    assert errors == [
        {
            "file_path": "A.lean",
            "message": build_output,
            "failed_dependencies": ["B.lean"],
        }
    ]


@pytest.mark.asyncio
async def test_prepare_tool_threads_structured_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    client = object()
    ctx: Any = SimpleNamespace(
        request_context=SimpleNamespace(lifespan_context=SimpleNamespace(client=client))
    )
    build_failure = "lake setup-file B.lean failed\nerror: C.lean:2:3: unknown identifier 'c'"

    async def setup_client_for_file(_ctx: Any, path: str) -> str:
        return path

    async def force_prepare(_client: Any, path: str) -> tuple[None, list[dict[str, Any]]]:
        if path == "A.lean":
            return None, []
        return None, [
            {
                "file_path": path,
                "message": build_failure,
                "failed_dependencies": ["C.lean"],
            }
        ]

    monkeypatch.setattr("paf.lean_mcp.server.setup_client_for_file", setup_client_for_file)
    monkeypatch.setattr("paf.lean_mcp._force_prepare_dependencies", force_prepare)

    result = await prepare_dependencies(ctx, ["A.lean", "B.lean", "B.lean"])

    assert result == {
        "attempted": ["A.lean", "B.lean"],
        "ready": ["A.lean"],
        "failed": ["B.lean"],
        "errors": [
            {
                "file_path": "B.lean",
                "message": build_failure,
                "failed_dependencies": ["C.lean"],
            }
        ],
    }


@pytest.mark.asyncio
async def test_concurrent_stale_files_share_completed_dependency_work(tmp_path: Path) -> None:
    for path in ("A.lean", "B.lean"):
        write(tmp_path, path, "import Mathlib\n")
    client = FakeClient(tmp_path)
    for path in ("A.lean", "B.lean"):
        document = await client.open(path, wait=False, dependency_build_mode="never")
        document.diagnostics = [{"message": "Imports are out of date"}]
    client.events.clear()
    build_started = asyncio.Event()
    release_build = asyncio.Event()

    async def barrier(_client: Any, path: str, timeout: float | None) -> None:
        client.events.append(("barrier", path, timeout))
        mode = client.open_modes[path]
        if path == "A.lean" and mode == "once":
            build_started.set()
            await release_build.wait()
            client._docs[path].diagnostics = []
        elif (
            path == "B.lean"
            and mode == "never"
            and sum(event[:2] == ("barrier", "B.lean") for event in client.events) > 1
        ):
            client._docs[path].diagnostics = []

    first = asyncio.create_task(barrier_with_dependency_refresh(client, "A.lean", original=barrier))
    await build_started.wait()
    second = asyncio.create_task(
        barrier_with_dependency_refresh(client, "B.lean", original=barrier)
    )
    await asyncio.sleep(0)
    release_build.set()
    await asyncio.gather(first, second)

    opens = [event for event in client.events if event[0] == "open"]
    assert opens == [
        ("open", "A.lean", False, "once"),
        ("open", "B.lean", False, "never"),
    ]
