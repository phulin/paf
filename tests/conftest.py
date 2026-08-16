import sqlite3
from pathlib import Path

import pytest

import paf.state_db as state_db


@pytest.fixture(autouse=True)
def fast_test_database(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep persistence semantics while avoiding disk durability waits in tests."""

    def connect(path: Path) -> sqlite3.Connection:
        connection = sqlite3.connect(path, timeout=30)
        connection.execute("PRAGMA journal_mode=MEMORY")
        connection.execute("PRAGMA synchronous=OFF")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    monkeypatch.setattr(state_db, "_connect", connect)
