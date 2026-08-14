from pathlib import Path

import pytest

from lastlib_swarm.run_lock import RepositoryRunLock


def test_repository_run_lock_excludes_another_state_directory(tmp_path: Path) -> None:
    first = RepositoryRunLock(tmp_path)
    second = RepositoryRunLock(tmp_path)
    first.acquire()
    try:
        with pytest.raises(ValueError, match="another lastlib-swarm run"):
            second.acquire()
    finally:
        first.release()

    second.acquire()
    second.release()
