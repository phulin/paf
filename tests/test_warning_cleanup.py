from pathlib import Path

import pytest

from paf.warning_cleanup import (
    WarningDiagnostic,
    apply_deterministic_warning_cleanup,
    revert_deterministic_warning_cleanup,
)


def warning(
    *,
    message: str,
    line: int = 1,
    column: int = 2,
    body: str = "",
) -> WarningDiagnostic:
    header = f"warning: Book/Chapter.lean:{line}:{column}: {message}"
    return WarningDiagnostic("Book/Chapter.lean", line, column, message, f"{header}\n{body}")


def apply(tmp_path: Path, source: str, *diagnostics: WarningDiagnostic):
    repo = tmp_path
    lean = repo / "lean"
    target = lean / "Book" / "Chapter.lean"
    target.parent.mkdir(parents=True)
    target.write_text(source, encoding="utf-8")
    result = apply_deterministic_warning_cleanup(
        repo_root=repo,
        lean_root=lean,
        scope=("lean/Book/Chapter.lean",),
        diagnostics=diagnostics,
    )
    return result, target.read_text(encoding="utf-8")


@pytest.mark.parametrize(
    ("source", "diagnostic", "expected"),
    [
        (
            "  simpa [foo] using h\n",
            warning(message="try 'simp' instead of 'simpa'"),
            "  simp [foo] using h\n",
        ),
        (
            "  push_neg at h\n",
            warning(message="`push_neg` has been deprecated. Prefer using `push Not` instead."),
            "  push Not at h\n",
        ),
        (
            "  aesop <;> simp\n",
            warning(
                message="Used `tac1 <;> tac2` where `(tac1; tac2)` would suffice",
                column=8,
            ),
            "  aesop ; simp\n",
        ),
        (
            "  simpa using h\n",
            warning(message="Try `simp at h` instead of `simpa using h`"),
            "  simp at h\n",
        ),
        (
            "  letI : P := proof\n",
            warning(
                message="Try this:",
                body="  letI̵\n\nThe goal is a proposition, so `let` is preferred over `letI`.",
            ),
            "  let : P := proof\n",
        ),
        (
            "  haveI : P := proof\n",
            warning(
                message="Try this:",
                body="  haveI̵\n\nThe goal is a proposition, so `have` is preferred over `haveI`.",
            ),
            "  have : P := proof\n",
        ),
    ],
)
def test_applies_allowlisted_token_edit(
    tmp_path: Path,
    source: str,
    diagnostic: WarningDiagnostic,
    expected: str,
) -> None:
    result, updated = apply(tmp_path, source, diagnostic)

    assert result.applied
    assert result.changed_paths == ("lean/Book/Chapter.lean",)
    assert updated == expected


def test_removes_atomic_argument_from_flat_simp_list(tmp_path: Path) -> None:
    diagnostic = warning(
        message="This simp argument is unused:",
        column=15,
        body="  unused\n\n  [apply] simp [first, unused, last]",
    )

    result, updated = apply(tmp_path, "  simp [first, unused, last]\n", diagnostic)

    assert result.applied
    assert updated == "  simp [first, last]\n"


@pytest.mark.parametrize(
    ("source", "diagnostic"),
    [
        ("  omega\n", warning(message="Try this:", body="  omega")),
        ("  simpa using h\n", warning(message="try 'simp' instead of 'simpa'", column=3)),
        (
            "  simp [foo (a, b), unused]\n",
            warning(
                message="This simp argument is unused:",
                column=20,
                body="  unused",
            ),
        ),
        (
            "  simp [unused]\n",
            warning(
                message="This simp argument is unused:",
                column=8,
                body="  unused",
            ),
        ),
        ("  change P\n", warning(message="'change P' tactic does nothing")),
    ],
)
def test_near_miss_falls_back_without_editing(
    tmp_path: Path,
    source: str,
    diagnostic: WarningDiagnostic,
) -> None:
    result, updated = apply(tmp_path, source, diagnostic)

    assert not result.applied
    assert "allowlist" in result.reason
    assert updated == source


def test_mixed_warning_batch_is_all_or_nothing(tmp_path: Path) -> None:
    recognized = warning(message="try 'simp' instead of 'simpa'", line=1)
    unknown = warning(message="Variable name `h` is not explicitly referenced.", line=2)

    result, updated = apply(tmp_path, "  simpa using h\n  theorem x := h\n", recognized, unknown)

    assert not result.applied
    assert updated == "  simpa using h\n  theorem x := h\n"


def test_rejects_out_of_scope_path(tmp_path: Path) -> None:
    diagnostic = WarningDiagnostic(
        "Other.lean",
        1,
        0,
        "try 'simp' instead of 'simpa'",
        "warning: Other.lean:1:0: try 'simp' instead of 'simpa'",
    )

    result, updated = apply(tmp_path, "  simpa\n", diagnostic)

    assert not result.applied
    assert "outside scope" in result.reason
    assert updated == "  simpa\n"


def test_accepts_utf8_byte_column_when_it_identifies_one_token(tmp_path: Path) -> None:
    diagnostic = warning(message="try 'simp' instead of 'simpa'", column=5)

    result, updated = apply(tmp_path, "α   simpa\n", diagnostic)  # noqa: RUF001

    assert result.applied
    assert updated == "α   simp\n"  # noqa: RUF001


def test_reverts_an_applied_cleanup(tmp_path: Path) -> None:
    result, updated = apply(
        tmp_path,
        "  simpa using h\n",
        warning(message="try 'simp' instead of 'simpa'"),
    )
    assert result.applied
    assert updated == "  simp using h\n"

    changed = revert_deterministic_warning_cleanup(repo_root=tmp_path, rewrites=result.rewrites)

    assert changed == ("lean/Book/Chapter.lean",)
    assert (tmp_path / changed[0]).read_text(encoding="utf-8") == "  simpa using h\n"


def test_revert_refuses_to_overwrite_a_later_change(tmp_path: Path) -> None:
    result, _updated = apply(
        tmp_path,
        "  simpa using h\n",
        warning(message="try 'simp' instead of 'simpa'"),
    )
    target = tmp_path / "lean" / "Book" / "Chapter.lean"
    target.write_text("  exact h\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="changed"):
        revert_deterministic_warning_cleanup(repo_root=tmp_path, rewrites=result.rewrites)

    assert target.read_text(encoding="utf-8") == "  exact h\n"
