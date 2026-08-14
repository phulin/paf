from pathlib import Path

from lastlib_swarm.scope import ScopeMatcher


def test_single_star_never_crosses_a_directory_boundary(tmp_path: Path) -> None:
    direct = tmp_path / "X" / "Direct.lean"
    nested = tmp_path / "X" / "nested" / "Nested.lean"
    direct.parent.mkdir(parents=True)
    nested.parent.mkdir(parents=True)
    direct.write_text("def direct := 1\n", encoding="utf-8")
    nested.write_text("def nested := 1\n", encoding="utf-8")
    matcher = ScopeMatcher(("X/*.lean",))

    assert matcher.files(tmp_path) == [direct]
    assert matcher.matches("X/Direct.lean")
    assert not matcher.matches("X/nested/Nested.lean")


def test_globstar_matches_zero_or_more_directories_consistently(tmp_path: Path) -> None:
    direct = tmp_path / "X" / "Direct.lean"
    nested = tmp_path / "X" / "nested" / "Nested.lean"
    direct.parent.mkdir(parents=True)
    nested.parent.mkdir(parents=True)
    direct.write_text("def direct := 1\n", encoding="utf-8")
    nested.write_text("def nested := 1\n", encoding="utf-8")
    matcher = ScopeMatcher(("X/**/*.lean",))

    assert matcher.files(tmp_path) == [direct, nested]
    assert matcher.matches("X/Direct.lean")
    assert matcher.matches("X/nested/Nested.lean")
