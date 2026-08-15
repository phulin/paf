from pathlib import Path

import pytest

from paf.adapters import LatexAdapter, MarkdownAdapter, SourceAdapter, TextAdapter
from paf.config import infer_config, load_config
from paf.models import SourceSpan
from tests.support import write_project


def test_markdown_atx_handles_duplicate_empty_and_unicode_headings(tmp_path: Path) -> None:
    source = tmp_path / "notes.md"
    source.write_text("# Notes\n\n## Résumé λ\n## Résumé λ\n\n## 最後\nbody\n", encoding="utf-8")
    adapter = MarkdownAdapter(root=tmp_path)

    first = adapter.discover_units(adapter.read_document(source))
    second = adapter.discover_units(adapter.read_document(source))

    assert isinstance(adapter, SourceAdapter)
    assert [unit.title for unit in first] == ["Résumé λ", "Résumé λ", "最後"]
    assert [unit.source_span for unit in first] == [
        SourceSpan(3, 3),
        SourceSpan(4, 5),
        SourceSpan(6, 7),
    ]
    assert (
        [unit.id for unit in first]
        == [unit.id for unit in second]
        == [
            "notes/unit-01",
            "notes/unit-02",
            "notes/unit-03",
        ]
    )


def test_markdown_numbered_compatibility_preserves_state_ids(tmp_path: Path) -> None:
    source = tmp_path / "07-book.md"
    source.write_text("# B\n## 2. Second\n## 10. Tenth\n", encoding="utf-8")
    adapter = MarkdownAdapter(root=tmp_path, profile="numbered-chapters", document_id="book07")

    units = adapter.discover_units(adapter.read_document(source))

    assert [(unit.id, unit.ordinal) for unit in units] == [
        ("book07/chapter-02", 2),
        ("book07/chapter-10", 10),
    ]


def test_markdown_atx_ignores_comments_and_fenced_code(tmp_path: Path) -> None:
    source = tmp_path / "notes.md"
    source.write_text(
        "<!--\n## Hidden\n-->\n```markdown\n## Also hidden\n```\n## Visible\n",
        encoding="utf-8",
    )
    adapter = MarkdownAdapter(root=tmp_path)

    units = adapter.discover_units(adapter.read_document(source))

    assert [(unit.title, unit.source_span) for unit in units] == [("Visible", SourceSpan(7, 7))]


def test_latex_ignores_comments_and_verbatim_and_keeps_line_spans(tmp_path: Path) -> None:
    source = tmp_path / "paper.tex"
    source.write_text(
        """\\title{Unicode π}
% \\section{Comment}
\\section{First}
\\begin{verbatim}
\\section{Not a unit}
\\end{verbatim}
text with \\% and a comment % \\section{Also hidden}
\\section*{Second λ}
\\section{Empty}
""",
        encoding="utf-8",
    )
    adapter = LatexAdapter(root=tmp_path)

    units = adapter.discover_units(adapter.read_document(source))

    assert [unit.title for unit in units] == ["First", "Second λ", "Empty"]
    assert [unit.source_span for unit in units] == [
        SourceSpan(3, 7),
        SourceSpan(8, 8),
        SourceSpan(9, 9),
    ]


def test_latex_boundaries_are_configurable(tmp_path: Path) -> None:
    source = tmp_path / "paper.tex"
    source.write_text("\\chapter{C}\n\\section{S}\n\\subsection{SS}\n", encoding="utf-8")

    adapter = LatexAdapter(root=tmp_path, commands=("chapter", "subsection"))
    units = adapter.discover_units(adapter.read_document(source))
    assert [unit.title for unit in units] == ["C", "SS"]


def test_latex_follows_local_inputs_only_when_enabled(tmp_path: Path) -> None:
    (tmp_path / "parts").mkdir()
    root = tmp_path / "main.tex"
    root.write_text(
        "% \\input{parts/commented}\n\\section{Root}\n\\input{parts/child}\n",
        encoding="utf-8",
    )
    (tmp_path / "parts" / "child.tex").write_text("\\section{Child}\n", encoding="utf-8")

    disabled = LatexAdapter(root=tmp_path)
    enabled = LatexAdapter(root=tmp_path, follow_includes=True)

    assert [unit.title for unit in disabled.discover_units(disabled.read_document(root))] == [
        "Root"
    ]
    followed = enabled.discover_units(enabled.read_document(root))
    assert [(unit.title, unit.source.as_posix()) for unit in followed] == [
        ("Root", "main.tex"),
        ("Child", "parts/child.tex"),
    ]


def test_latex_rejects_include_cycles(tmp_path: Path) -> None:
    (tmp_path / "a.tex").write_text("\\input{b}\n", encoding="utf-8")
    (tmp_path / "b.tex").write_text("\\include{a.tex}\n", encoding="utf-8")

    with pytest.raises(ValueError, match=r"include cycle: a\.tex -> b\.tex -> a\.tex"):
        LatexAdapter(root=tmp_path, follow_includes=True).read_document(tmp_path / "a.tex")


def test_text_whole_file_heading_pattern_and_delimiter_modes(tmp_path: Path) -> None:
    source = tmp_path / "appendix.txt"
    source.write_text("CHAPTER 1: 第一\nbody\nCHAPTER 2: 第二\n", encoding="utf-8")

    whole = TextAdapter(root=tmp_path)
    headings = TextAdapter(
        root=tmp_path,
        heading_pattern=r"^CHAPTER (?P<number>\d+): (?P<title>.+)$",
    )

    assert [unit.source_span for unit in whole.discover_units(whole.read_document(source))] == [
        SourceSpan(1, 3)
    ]
    assert [unit.title for unit in headings.discover_units(headings.read_document(source))] == [
        "第一",
        "第二",
    ]

    source.write_text("one\n---\ntwo\n", encoding="utf-8")
    delimiter = TextAdapter(root=tmp_path, delimiter="---")
    assert len(delimiter.discover_units(delimiter.read_document(source))) == 2


def test_sources_rules_select_adapter_and_path_specific_splitting(tmp_path: Path) -> None:
    config_path = write_project(tmp_path)
    tex = tmp_path / "books" / "book.tex"
    tex.write_text("\\chapter{Ignored}\n\\section{Selected}\n", encoding="utf-8")
    config = config_path.read_text(encoding="utf-8").replace(
        'source = "books/book.md"', 'source = "books/book.tex"'
    )
    config = (
        """[sources]
unit = "chapter"

[[sources.rules]]
glob = "books/*.tex"
format = "latex"
unit = "section"

"""
        + config
    )
    config_path.write_text(config, encoding="utf-8")

    loaded = load_config(config_path)

    assert loaded.books[0].format == "latex"
    assert loaded.books[0].unit == "section"
    assert [unit.title for unit in loaded.work_units] == ["Selected"]
    assert loaded.source_rules[0]["glob"] == "books/*.tex"


@pytest.mark.parametrize(
    ("name", "content", "titles"),
    [
        ("source.md", "# T\n## 1. One\n", ["One"]),
        ("source.tex", "\\section{One}\n", ["One"]),
        ("source.txt", "plain text\n", ["Source"]),
    ],
)
def test_single_file_inference_supports_all_adapters(
    tmp_path: Path, name: str, content: str, titles: list[str]
) -> None:
    (tmp_path / ".git").mkdir()
    source = tmp_path / name
    source.write_text(content, encoding="utf-8")

    config = infer_config(source)

    assert [unit.title for unit in config.work_units] == titles
    if name.endswith(".md"):
        assert config.work_units[0].id == "source/chapter-01"
