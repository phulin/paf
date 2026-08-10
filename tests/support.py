from pathlib import Path

from lastlib_swarm.models import Stage


def write_project(tmp_path: Path, *, chapters: str = "") -> Path:
    (tmp_path / "books").mkdir()
    (tmp_path / "prompts").mkdir()
    (tmp_path / "books" / "book.md").write_text(
        "# Book\n\n## 1. First chapter\n\nText.\n\n## 2. Second chapter\n",
        encoding="utf-8",
    )
    for stage in Stage:
        (tmp_path / "prompts" / f"{stage}.md").write_text(
            "Do {book_title} chapter {chapter_number_padded}", encoding="utf-8"
        )
    config = tmp_path / "swarm.toml"
    config.write_text(
        f"""
[swarm]
repo = "."
max_agents = 4

[stages.formalize]
prompt = "prompts/formalize.md"
[stages.review]
prompt = "prompts/review.md"
[stages.prove]
prompt = "prompts/prove.md"
[stages.repair]
prompt = "prompts/repair.md"

[[books]]
id = "book"
title = "A Book"
source = "books/book.md"
lean_root = "lean/Book"
module = "Book"
{chapters}
""",
        encoding="utf-8",
    )
    return config
