# Reconcile the build: {book_title}, chapter {chapter_number}

## Mission

Read chapter {chapter_number}, “{chapter_title},” in `{source}`, every assigned Lean file, and the
coordinator build or review feedback appended below. Make one minimal edit transaction that moves
the project toward a clean Lake build and a faithful common API.

This is an elaboration-only stage. Never prove a theorem, lemma, or other proposition. Replace an
obstructing proof body in its entirety with `by sorry`. Spend effort only on imports, declaration
signatures, types, structures, instances, definitions, and API compatibility.

## Fixup standard

Treat the appended coordinator feedback as authoritative. Resolve unknown declarations, invalid
types, missing hypotheses, import cycles, incompatible provisional APIs, unsolved non-proof terms,
and unexpected warnings owned by the assigned scope. Do not improve, complete, or debug proofs.

Preserve the intended mathematical strength. Do not weaken a correct conclusion merely to compile,
add the desired result as an assumption, or introduce a declaration engineered to imply it. Mark a
substantive defect in the informal source with a precise `SOURCE_ISSUE` comment.

## Workflow

1. Read all appended Lake diagnostics and review findings before editing.
2. Identify which reported failures are owned by the assigned scope.
3. Visit files in their observed import order. Request whole-file MCP diagnostics whenever entering
   a file and again after every edit.
4. Reconcile provisional names and interfaces with canonical earlier LastLib and pinned Mathlib APIs.
5. Make one coherent batch of minimal changes, replacing every obstructing proof body with `by sorry`.
6. Report every resolved and unresolved diagnostic precisely.

Do not run Lean, Lake, or another language server. Use only the attached Lean MCP for interactive
diagnostics. After this transaction, the coordinator rescans imports, runs the authoritative Lake
build when its refined predecessors are clean, and publishes the cache before releasing descendants.
Unrelated dependency-ready agents may continue running while this result is integrated.

## Definition of done

The assigned scope addresses every applicable coordinator diagnostic or review finding without
concealing proof obligations or changing unrelated mathematics. Remaining blockers are reported with
their exact owner and required dependency.
