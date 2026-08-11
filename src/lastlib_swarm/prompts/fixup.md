# Reconcile the build: {book_title}, chapter {chapter_number}

## Mission

Read chapter {chapter_number}, “{chapter_title},” in `{source}`, every assigned Lean file, and the
coordinator build or review feedback appended below. Make the smallest coherent source changes that
move the complete project toward a clean Lake build and a faithful common API.

This is the only pre-proof stage that may change declaration interfaces. Unfinished proof bodies may
use `by sorry`; the immediate compiler invariant is that every declaration signature elaborates.

## Fixup standard

Treat the appended coordinator feedback as authoritative. Resolve unknown declarations, invalid
types, missing hypotheses, import cycles, incompatible provisional APIs, unsolved non-proof terms,
and unexpected warnings owned by the assigned scope. Replace an unfinished proof attempt with
`by sorry` when its body is the only obstruction.

Preserve the intended mathematical strength. Do not weaken a correct conclusion merely to compile,
add the desired result as an assumption, or introduce a declaration engineered to imply it. Mark a
substantive defect in the informal source with a precise `SOURCE_ISSUE` comment.

## Workflow

1. Read all appended Lake diagnostics and review findings before editing.
2. Identify which reported failures are owned by the assigned scope.
3. Reconcile provisional names and interfaces with canonical earlier LastLib and pinned Mathlib APIs.
4. Make one coherent batch of minimal changes, using `by sorry` for unfinished proofs.
5. Report every resolved and unresolved diagnostic precisely; the coordinator will run Lake again.

Do not run Lean, Lake, or a language server. Do not request LSP diagnostics. The coordinator alone
builds the shared project after the complete parallel fixup batch and supplies fresh diagnostics to
the next iteration.

## Definition of done

The assigned scope addresses every applicable coordinator diagnostic or review finding without
concealing proof obligations or changing unrelated mathematics. Remaining blockers are reported with
their exact owner and required dependency.
