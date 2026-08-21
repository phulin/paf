from pathlib import Path

import pytest

from paf.config import (
    infer_config,
    infer_corpus,
    load_config,
    parse_book_dependencies,
)
from paf.models import Stage
from tests.support import write_project


def test_discovers_chapters_and_renders_paths(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path))

    assert [chapter.id for chapter in config.chapters] == [
        "book/chapter-01",
        "book/chapter-02",
    ]
    first = config.chapters[0]
    assert first.chapter_module == "Book.Chapter01"
    assert first.scope == ("lean/Book/Chapter01.lean", "lean/Book/Chapter01/**/*.lean")
    assert config.stages[Stage.DISCOVER].max_rounds == 1
    assert config.stages[Stage.FORMALIZE].max_rounds == 3
    assert config.stages[Stage.FORMALIZE].max_rounds == 3
    assert config.stages[Stage.REVIEW].max_rounds == 3
    assert config.stages[Stage.DISCOVER].model == "gpt-5.6-luna"
    assert config.stages[Stage.DISCOVER].reasoning_effort == "xhigh"
    assert config.stages[Stage.FORMALIZE].model is None
    assert config.stages[Stage.FORMALIZE].reasoning_effort is None
    assert config.model_for(Stage.FORMALIZE) == config.settings.model
    assert config.reasoning_effort_for(Stage.FORMALIZE) == config.settings.reasoning_effort
    assert config.settings.state_dir == tmp_path / ".paf"
    assert config.stages[Stage.DISCOVER].max_agents == 40
    assert config.stages[Stage.PROVE].chunk_size == 6
    assert config.stages[Stage.PROVE].unchanged_retry_limit == 2
    assert config.stages[Stage.REVIEW].chunk_size is None
    assert config.settings.lean_project == Path("lean")
    assert config.settings.lean_mcp_tool_timeout_seconds == 300
    assert config.settings.capacity_resume_attempts == 10
    assert config.settings.capacity_resume_delay_seconds == 15
    assert config.settings.capacity_resume_max_delay_seconds == 120
    assert config.settings.codex_fd_recycle_threshold == 256
    assert config.settings.codex_fd_recycle_attempts == 20
    assert config.settings.sandbox == "danger-full-access"
    assert config.settings.cache_compaction_layers == 32
    assert not hasattr(config.settings, "interface_invalidation")
    assert config.steward.model == "gpt-5.6-sol"
    assert config.steward.enabled is False
    assert config.steward.reasoning_effort == "medium"
    assert config.steward.worker_model == "gpt-5.6-sol"
    assert config.steward.worker_reasoning_effort == "medium"
    assert config.steward.max_concurrent_packages_per_work_unit == 1


def test_loads_and_validates_discovery_concurrency(tmp_path: Path) -> None:
    path = write_project(tmp_path)
    path.write_text(
        path.read_text(encoding="utf-8").replace(
            'prompt = "prompts/discover.md"',
            'prompt = "prompts/discover.md"\nmax_agents = 12',
        ),
        encoding="utf-8",
    )

    assert load_config(path).stages[Stage.DISCOVER].max_agents == 12

    path.write_text(
        path.read_text(encoding="utf-8").replace("max_agents = 12", "max_agents = 0"),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match=r"stages\.discover\.max_agents"):
        load_config(path)


def test_loads_and_validates_proof_chunk_size(tmp_path: Path) -> None:
    path = write_project(tmp_path)
    path.write_text(
        path.read_text(encoding="utf-8").replace(
            'prompt = "prompts/prove.md"',
            'prompt = "prompts/prove.md"\nchunk_size = 7',
        ),
        encoding="utf-8",
    )

    assert load_config(path).stages[Stage.PROVE].chunk_size == 7

    path.write_text(
        path.read_text(encoding="utf-8").replace("chunk_size = 7", "chunk_size = 0"),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match=r"stages\.prove\.chunk_size"):
        load_config(path)


def test_loads_and_validates_global_unchanged_proof_retry_limit(tmp_path: Path) -> None:
    path = write_project(tmp_path)
    path.write_text(
        path.read_text(encoding="utf-8").replace(
            'prompt = "prompts/prove.md"',
            'prompt = "prompts/prove.md"\nunchanged_retry_limit = 4',
        ),
        encoding="utf-8",
    )

    assert load_config(path).stages[Stage.PROVE].unchanged_retry_limit == 4

    path.write_text(
        path.read_text(encoding="utf-8").replace(
            "unchanged_retry_limit = 4", "unchanged_retry_limit = 0"
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match=r"stages\.prove\.unchanged_retry_limit"):
        load_config(path)


def test_loads_and_validates_steward_settings(tmp_path: Path) -> None:
    path = write_project(tmp_path)
    text = path.read_text(encoding="utf-8")
    path.write_text(
        text
        + """
[steward]
enabled = true
model = "strong-planner"
reasoning_effort = "xhigh"
worker_model = "cheap-editor"
worker_reasoning_effort = "max"
lease_ttl_seconds = 1200
maximum_worker_steps = 24
max_concurrent_packages_per_work_unit = 2
""",
        encoding="utf-8",
    )

    steward = load_config(path).steward

    assert steward.enabled is False
    assert steward.model == "strong-planner"
    assert steward.worker_model == "cheap-editor"
    assert steward.worker_reasoning_effort == "max"
    assert steward.lease_ttl_seconds == 1200
    assert steward.maximum_worker_steps == 24
    assert steward.max_concurrent_packages_per_work_unit == 2

    path.write_text(
        path.read_text(encoding="utf-8").replace(
            "lease_ttl_seconds = 1200", "lease_ttl_seconds = 0"
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match=r"steward\.lease_ttl_seconds"):
        load_config(path)

    path.write_text(
        path.read_text(encoding="utf-8")
        .replace("lease_ttl_seconds = 0", "lease_ttl_seconds = 1200")
        .replace(
            "max_concurrent_packages_per_work_unit = 2",
            "max_concurrent_packages_per_work_unit = 0",
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match=r"steward\.max_concurrent_packages_per_work_unit"):
        load_config(path)


def test_shepherd_table_is_a_temporary_steward_alias(tmp_path: Path) -> None:
    path = write_project(tmp_path)
    path.write_text(
        path.read_text(encoding="utf-8") + "\n[shepherd]\nenabled = false\n",
        encoding="utf-8",
    )

    assert load_config(path).steward.enabled is False


def test_removed_sweep_configuration_is_rejected(tmp_path: Path) -> None:
    path = write_project(tmp_path)
    path.write_text(
        path.read_text(encoding="utf-8") + "\n[steward]\nfailure_threshold = 2\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="repair-sweep settings were removed"):
        load_config(path)


def test_selects_configured_chapters(tmp_path: Path) -> None:
    config = load_config(write_project(tmp_path, chapters="chapters = [2]"))

    assert [chapter.number for chapter in config.chapters] == [2]


def test_loads_phase_specific_book_effort(tmp_path: Path) -> None:
    path = write_project(tmp_path)
    text = path.read_text(encoding="utf-8").replace(
        'module = "Book"',
        'module = "Book"\nstatement_effort = 2.5\nproof_effort = 9',
    )
    path.write_text(text, encoding="utf-8")

    book = load_config(path).books[0]

    assert book.statement_effort == 2.5
    assert book.proof_effort == 9


def test_rejects_unknown_isolation_backend(tmp_path: Path) -> None:
    path = write_project(tmp_path)
    path.write_text(
        path.read_text(encoding="utf-8").replace('isolation = "shared"', 'isolation = "telepathy"'),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"swarm\.isolation"):
        load_config(path)


def test_loads_and_validates_cache_compaction_threshold(tmp_path: Path) -> None:
    path = write_project(tmp_path)
    path.write_text(
        path.read_text(encoding="utf-8").replace(
            'isolation = "shared"',
            'isolation = "shared"\ncache_compaction_layers = 8',
        ),
        encoding="utf-8",
    )

    assert load_config(path).settings.cache_compaction_layers == 8

    path.write_text(
        path.read_text(encoding="utf-8").replace(
            "cache_compaction_layers = 8", "cache_compaction_layers = 1"
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match=r"swarm\.cache_compaction_layers"):
        load_config(path)


def test_rejects_removed_interface_invalidation_mode(tmp_path: Path) -> None:
    path = write_project(tmp_path)
    path.write_text(
        path.read_text(encoding="utf-8").replace(
            'isolation = "shared"',
            'isolation = "shared"\ninterface_invalidation = "observe"',
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match=r"swarm\.interface_invalidation was removed"):
        load_config(path)


def test_loads_lean_mcp_settings(tmp_path: Path) -> None:
    path = write_project(tmp_path)
    path.write_text(
        path.read_text(encoding="utf-8").replace(
            'isolation = "shared"',
            'isolation = "shared"\nlean_project = "lean-project"\n'
            "lean_mcp_tool_timeout_seconds = 45",
        ),
        encoding="utf-8",
    )

    settings = load_config(path).settings

    assert settings.lean_project == Path("lean-project")
    assert settings.lean_mcp_tool_timeout_seconds == 45


def test_rejects_removed_lean_mcp_setting(tmp_path: Path) -> None:
    path = write_project(tmp_path)
    path.write_text(
        path.read_text(encoding="utf-8").replace(
            'isolation = "shared"', 'isolation = "shared"\nlean_mcp = false'
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"swarm\.lean_mcp was removed"):
        load_config(path)


def test_rejects_lean_project_outside_repository(tmp_path: Path) -> None:
    path = write_project(tmp_path)
    path.write_text(
        path.read_text(encoding="utf-8").replace(
            'isolation = "shared"', 'isolation = "shared"\nlean_project = "../outside"'
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"swarm\.lean_project"):
        load_config(path)


def test_rejects_unknown_book_dependency(tmp_path: Path) -> None:
    path = write_project(tmp_path)
    text = path.read_text(encoding="utf-8").replace(
        'module = "Book"', 'module = "Book"\ndepends_on = ["missing"]'
    )
    path.write_text(text, encoding="utf-8")

    with pytest.raises(ValueError, match="unconfigured books"):
        load_config(path)


def test_infers_zero_config_project_from_markdown(tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()
    books = tmp_path / "books"
    books.mkdir()
    target = books / "07-example-theory.md"
    target.write_text(
        "# Example Theory\n\n## 1. Foundations\n\n## 2. Main result\n", encoding="utf-8"
    )
    existing = tmp_path / "lean" / "LastLib" / "Book07ExistingAPI"
    existing.mkdir(parents=True)

    config = infer_config(target)

    assert config.settings.repo == tmp_path
    assert config.settings.model == "gpt-5.6-luna"
    assert config.settings.reasoning_effort == "xhigh"
    assert config.settings.bypass_approvals_and_sandbox
    assert config.settings.isolation == "auto"
    assert config.steward.enabled is True
    assert config.settings.state_dir == tmp_path / ".paf" / "book07"
    assert config.books[0].module == "LastLib.Book07ExistingAPI"
    assert [chapter.number for chapter in config.chapters] == [1, 2]
    assert all(stage.prompt.is_file() for stage in config.stages.values())


def test_config_stage_prompts_are_optional(tmp_path: Path) -> None:
    path = write_project(tmp_path)
    text = path.read_text(encoding="utf-8")
    start = text.index("[stages.formalize]")
    end = text.index("[[books]]")
    path.write_text(text[:start] + text[end:], encoding="utf-8")

    config = load_config(path)

    assert config.stages[Stage.FORMALIZE].prompt.name == "formalize.md"


def test_loads_stage_specific_model_and_reasoning_effort(tmp_path: Path) -> None:
    path = write_project(tmp_path)
    path.write_text(
        path.read_text(encoding="utf-8").replace(
            '[stages.review]\nprompt = "prompts/review.md"',
            '[stages.review]\nprompt = "prompts/review.md"\n'
            'model = "gpt-5.6-sol"\nreasoning_effort = "high"',
        ),
        encoding="utf-8",
    )

    config = load_config(path)

    assert config.model_for(Stage.REVIEW) == "gpt-5.6-sol"
    assert config.reasoning_effort_for(Stage.REVIEW) == "high"
    assert config.model_for(Stage.PROVE) == config.settings.model
    assert config.reasoning_effort_for(Stage.PROVE) == config.settings.reasoning_effort


@pytest.mark.parametrize(("key", "value"), (("model", 7), ("reasoning_effort", True)))
def test_rejects_non_string_stage_agent_settings(tmp_path: Path, key: str, value: object) -> None:
    path = write_project(tmp_path)
    path.write_text(
        path.read_text(encoding="utf-8").replace(
            '[stages.review]\nprompt = "prompts/review.md"',
            f'[stages.review]\nprompt = "prompts/review.md"\n{key} = {str(value).lower()}',
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=rf"stages\.review\.{key}"):
        load_config(path)


def test_legacy_repair_stage_config_maps_to_formalize(tmp_path: Path) -> None:
    path = write_project(tmp_path)
    text = path.read_text(encoding="utf-8")
    text = text.replace('[stages.discover]\nprompt = "prompts/discover.md"\n', "")
    text = text.replace("[stages.formalize]", "[stages.repair]")
    path.write_text(text, encoding="utf-8")

    config = load_config(path)

    assert config.stages[Stage.FORMALIZE].prompt.name == "formalize.md"


def test_infers_multiple_books_and_chained_mermaid_dependencies(tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()
    books = tmp_path / "books"
    books.mkdir()
    for number in range(1, 4):
        (books / f"{number:02d}-book.md").write_text(
            f"# Book {number}\n\n## 1. Chapter\n", encoding="utf-8"
        )
    graph = tmp_path / "BOOK_DEPENDENCIES.md"
    graph.write_text("B01 --> B02 --> B03\n", encoding="utf-8")

    assert parse_book_dependencies(graph) == {
        "book02": ("book01",),
        "book03": ("book02",),
    }
    config = infer_corpus((books,))

    assert [book.id for book in config.books] == ["book01", "book02", "book03"]
    assert config.books[2].depends_on == ("book02",)
    assert config.settings.state_dir.parent == tmp_path / ".paf"


def test_loads_recursive_sources_only_config_with_rules_dependencies_and_manifest(
    tmp_path: Path,
) -> None:
    (tmp_path / ".git").mkdir()
    (tmp_path / "notes" / "nested").mkdir(parents=True)
    (tmp_path / "notes" / "nested" / "lecture.tex").write_text(
        "\\chapter{Ignored}\n\\section{Lecture}\n", encoding="utf-8"
    )
    (tmp_path / "notes" / "intro.md").write_text("# Intro\n## Start\n", encoding="utf-8")
    config_path = tmp_path / "paf.toml"
    config_path.write_text(
        """[swarm]
repo = "."
isolation = "shared"

[sources]
roots = ["notes"]
include = ["**/*.md", "**/*.tex"]
exclude = ["**/drafts/**"]
manifest = ["notes/nested/lecture.tex", "notes/intro.md"]

[sources.dependencies]
"notes/nested/lecture.tex" = ["notes/intro.md"]

[[sources.rules]]
glob = "notes/**/*.tex"
format = "latex"
unit = "section"
""",
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert [book.source.as_posix() for book in config.books] == [
        "notes/nested/lecture.tex",
        "notes/intro.md",
    ]
    assert config.books[0].depends_on == (config.books[1].id,)
    assert [unit.title for unit in config.work_units] == ["Lecture", "Start"]
    assert config.source_roots == (Path("notes"),)


def test_loads_manifest_extracted_from_an_ordering_source(tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()
    (tmp_path / "books").mkdir()
    (tmp_path / "books" / "a.tex").write_text("\\section{A}\n", encoding="utf-8")
    (tmp_path / "books" / "b.tex").write_text("\\section{B}\n", encoding="utf-8")
    (tmp_path / "contents.tex").write_text("\\book{b}\n\\book{a}\n", encoding="utf-8")
    config_path = tmp_path / "paf.toml"
    config_path.write_text(
        r"""[swarm]
repo = "."
isolation = "shared"

[sources]
roots = ["books"]

[sources.manifest]
path = "contents.tex"
pattern = '\\book\{(?P<name>[^}]+)\}'
template = "books/{name}.tex"
""",
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert [document.path.as_posix() for document in config.documents] == [
        "books/b.tex",
        "books/a.tex",
    ]


def test_inferred_directory_recurses_over_mixed_formats_but_direct_markdown_stays_legacy(
    tmp_path: Path,
) -> None:
    (tmp_path / ".git").mkdir()
    (tmp_path / "sources" / "deep").mkdir(parents=True)
    markdown = tmp_path / "sources" / "deep" / "01-notes.md"
    markdown.write_text("# Notes\n## Unnumbered heading\n", encoding="utf-8")
    (tmp_path / "sources" / "appendix.txt").write_text("plain\n", encoding="utf-8")
    (tmp_path / "sources" / "theory.tex").write_text("\\section{Theory}\n", encoding="utf-8")

    corpus = infer_corpus((tmp_path / "sources",))
    direct = infer_config(markdown)

    assert [book.format for book in corpus.books] == ["text", "markdown", "latex"]
    assert [unit.title for unit in corpus.work_units] == [
        "Appendix",
        "Unnumbered heading",
        "Theory",
    ]
    assert direct.books[0].adapter_profile == "numbered-chapters"
    assert direct.work_units == ()
