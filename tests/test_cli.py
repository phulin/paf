from pathlib import Path

import pytest

from lastlib_swarm.cli import main, select_chapters
from lastlib_swarm.config import load_config
from tests.support import write_project


def test_selects_book_and_chapter_number(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path))

    chapters = select_chapters(config, books=["book"], chapter_selectors=["2"])

    assert [chapter.id for chapter in chapters] == ["book/chapter-02"]


def test_plan_command(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    path = write_project(tmp_path)

    assert main(["plan", "--config", str(path)]) == 0
    assert "Statement work is chapter-pipelined" in capsys.readouterr().out
