from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from lastlib_swarm.lean_mcp import (
    barrier_with_dependency_refresh,
    record_stale_dependency,
    reload_with_dependencies_when_stale,
)


@dataclass
class FakeDocument:
    path: str
    uri: str
    text: str
    stale_imports: bool = False
    diagnostics: list[dict[str, Any]] = field(default_factory=list)


class FakeClient:
    def __init__(self, root: Path) -> None:
        self.project_path = str(root)
        self._docs: dict[str, FakeDocument] = {}
        self._docs_by_uri: dict[str, FakeDocument] = {}
        self.events: list[tuple[Any, ...]] = []
        self.open_modes: dict[str, str] = {}

    async def open(
        self, path: str, *, wait: bool, dependency_build_mode: str
    ) -> FakeDocument:
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
    assert client._docs["A.lean"].diagnostics == []


@pytest.mark.asyncio
async def test_failed_stale_recovery_is_not_repeated_without_new_state(tmp_path: Path) -> None:
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

    await barrier_with_dependency_refresh(client, "A.lean", original=barrier)
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
    await barrier_with_dependency_refresh(client, "A.lean", original=barrier)
    assert [event for event in client.events if event[0] == "open"] == [
        ("open", "A.lean", False, "once"),
        ("open", "A.lean", False, "once"),
    ]


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

    first = asyncio.create_task(
        barrier_with_dependency_refresh(client, "A.lean", original=barrier)
    )
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
