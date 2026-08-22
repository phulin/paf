from pathlib import Path

from paf.models import Stage


def write_project(tmp_path: Path, *, chapters: str = "") -> Path:
    (tmp_path / "books").mkdir()
    (tmp_path / "prompts").mkdir()
    (tmp_path / "lean").mkdir()
    (tmp_path / "lean" / "lean-toolchain").write_text("leanprover/lean4:stable\n", encoding="utf-8")
    (tmp_path / "lean" / "lakefile.toml").write_text('name = "test"\n', encoding="utf-8")
    fake_beam = tmp_path / "fake-lean-beam"
    fake_beam.write_text(
        """#!/usr/bin/env python3
import json
import signal
import time
import sys

if "ensure" in sys.argv and "--hold" in sys.argv:
    print(json.dumps({"result": {"ready": True}}), flush=True)
    signal.signal(signal.SIGTERM, lambda *_args: sys.exit(0))
    while True:
        time.sleep(1)
print(json.dumps({"result": {"ok": True}}))
""",
        encoding="utf-8",
    )
    fake_beam.chmod(0o755)
    (tmp_path / "books" / "book.md").write_text(
        "# Book\n\n## 1. First chapter\n\nText.\n\n## 2. Second chapter\n",
        encoding="utf-8",
    )
    for stage in Stage:
        (tmp_path / "prompts" / f"{stage}.md").write_text(
            "Do {book_title} chapter {chapter_number_padded}", encoding="utf-8"
        )
    config = tmp_path / "paf.toml"
    config.write_text(
        f"""
[swarm]
repo = "."
max_agents = 4
isolation = "shared"

[stages.formalize]
prompt = "prompts/formalize.md"
[stages.discover]
prompt = "prompts/discover.md"
[stages.review]
prompt = "prompts/review.md"
[stages.prove]
prompt = "prompts/prove.md"

[backend]
kind = "lean"
project = "lean"
beam_command = "{fake_beam}"

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
