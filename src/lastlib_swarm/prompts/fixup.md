# Reconcile the build: {book_title}, chapter {chapter_number}

## Mission

Start from the coordinator build or review feedback appended below. Read the implicated declarations,
their required prerequisites and affected dependents, and the relevant part of chapter
{chapter_number}, “{chapter_title},” in `{source}` when mathematical intent is needed. Do not reread
the complete chapter or assigned file set merely because this is a new attempt. Make one minimal edit
transaction that moves the project toward a clean Lake build and a faithful common API.

This is an elaboration-only stage. Never prove a theorem, lemma, or other proposition. Replace an
obstructing proof body in its entirety with `by sorry`. Spend effort only on imports, declaration
signatures, types, structures, instances, definitions, and API compatibility.

## Fixup standard

Treat the appended coordinator feedback as authoritative starting evidence, while recognizing that a
concurrent prerequisite repair may have made a diagnostic stale. Resolve current unknown
declarations, invalid types, missing hypotheses, import cycles, incompatible provisional APIs,
unsolved non-proof terms, and unexpected warnings owned by the assigned scope. Do not improve,
complete, or debug proofs.

Preserve the intended mathematical strength. Do not weaken a correct conclusion merely to compile,
add the desired result as an assumption, or introduce a declaration engineered to imply it. Record a
substantive defect in the informal source in the structured `source_issues` ledger; do not leave a
source-issue comment in Lean.

## Workflow

1. Read all appended diagnostics from the initial post-draft build before editing.
2. Identify which reported failures are owned by the assigned scope.
3. Visit files in their observed import order. Treat the coordinator feedback as the initial
   diagnostic pass: do not request diagnostics merely because you entered or switched files.
4. Reconcile provisional names and interfaces with canonical earlier LastLib and pinned Mathlib APIs.
5. Make one coherent batch of minimal changes, replacing every obstructing proof body with `by sorry`.
   Track the edited files and their assigned transitive dependents.
6. After the last relevant edit, request whole-file MCP diagnostics for that edited dependency closure
   in import order. If a repair invalidates a file already checked, recheck only that file and its
   affected dependents.
7. Report every resolved and unresolved diagnostic precisely.

Do not run Lean, Lake, or another language server. Use only the attached Lean MCP for interactive
diagnostics. After this transaction, the coordinator rescans imports, runs the authoritative Lake
build when its refined predecessors are clean, and publishes the cache before releasing descendants.
Unrelated dependency-ready agents may continue running while this result is integrated.

## Definition of done

The assigned scope addresses every applicable coordinator diagnostic or review finding without
concealing proof obligations or changing unrelated mathematics. Remaining blockers are reported with
their exact owner and required dependency. Here `complete` means that every supplied finding still
applicable to this scope has been addressed or precisely routed; it does not claim that the project
build is clean. The coordinator's subsequent Lake build is authoritative.
