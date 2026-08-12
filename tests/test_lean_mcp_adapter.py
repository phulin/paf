from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from lastlib_swarm.lean_mcp import reload_with_dependencies_when_stale


@dataclass
class FakeDocument:
    stale_imports: bool = False


class FakeClient:
    def __init__(self) -> None:
        self._docs: dict[str, FakeDocument] = {}
        self.events: list[tuple[Any, ...]] = []

    async def close_file(self, path: str) -> None:
        self.events.append(("close", path))
        self._docs.pop(path, None)

    async def open(self, path: str, *, wait: bool, dependency_build_mode: str) -> FakeDocument:
        self.events.append(("open", path, wait, dependency_build_mode))
        document = FakeDocument()
        self._docs[path] = document
        return document


@pytest.mark.asyncio
async def test_prepares_dependencies_on_first_open_and_after_stale_notification() -> None:
    client = FakeClient()

    async def original(_client: Any, path: str, wait: bool) -> FakeDocument:
        client.events.append(("reload", path, wait))
        return client._docs.setdefault(path, FakeDocument())

    await reload_with_dependencies_when_stale(client, "A.lean", original=original)
    await reload_with_dependencies_when_stale(client, "A.lean", original=original)
    await reload_with_dependencies_when_stale(client, "B.lean", original=original)
    await reload_with_dependencies_when_stale(client, "A.lean", original=original)
    client._docs["A.lean"].stale_imports = True
    await reload_with_dependencies_when_stale(client, "A.lean", original=original)

    assert client.events == [
        ("open", "A.lean", False, "once"),
        ("reload", "A.lean", False),
        ("open", "B.lean", False, "once"),
        ("reload", "A.lean", False),
        ("close", "A.lean"),
        ("open", "A.lean", False, "once"),
    ]
