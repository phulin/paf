from __future__ import annotations

import fcntl
import os
from pathlib import Path
from typing import TextIO


class RepositoryRunLock:
    """Prevent independent swarm state directories from sharing one worktree."""

    def __init__(self, repo: Path) -> None:
        self.path = repo / ".swarm" / "repository-run.lock"
        self._handle: TextIO | None = None

    def acquire(self) -> None:
        if self._handle is not None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            handle.seek(0)
            owner = handle.read().strip()
            handle.close()
            detail = f" ({owner})" if owner else ""
            raise ValueError(
                f"another lastlib-swarm run already owns {self.path}{detail}"
            ) from None
        handle.seek(0)
        handle.truncate()
        handle.write(f"pid {os.getpid()}\n")
        handle.flush()
        self._handle = handle

    def release(self) -> None:
        if self._handle is None:
            return
        handle = self._handle
        self._handle = None
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()
