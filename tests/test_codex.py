from pathlib import Path

from lastlib_swarm.codex import CodexExecutor, count_placeholders, render_prompt
from lastlib_swarm.config import load_config
from lastlib_swarm.state import StateStore, TokenUsage
from tests.support import write_project


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
    assert usage.api_tokens == 150
    assert usage.cached_input_tokens == 40
    assert usage.reasoning_output_tokens == 12
    assert usage.measured


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


def test_executor_uses_machine_readable_codex_mode(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [1]"))
    executor = CodexExecutor(config, StateStore(config))

    command = executor.command()

    assert command[:2] == ["codex", "exec"]
    assert "--json" in command
    assert "--output-schema" in command
    assert "--approve-for-me" in command
    assert "--dangerously-bypass-approvals-and-sandbox" not in command
    assert render_prompt(
        "Chapter {chapter_number_padded}: {chapter_title}", config.chapters[0]
    ) == ("Chapter 01: First chapter")
