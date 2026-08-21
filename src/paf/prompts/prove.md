# Prove assigned declarations: {book_title}, chapter {chapter_number}

## Mission

Replace exactly the assigned proof holes with Lean-checked proofs. Preserve established public
interfaces and unrelated holes. The Lean statement is the proof target; `{source}` is the default
mathematical argument, not authority for a stronger claim.

## First decision

Before broad search, classify the assignment from its current source and handoff:

- proceed when an exact lemma, focused proof route, or materially new retry strategy exists;
- report `statement_defect` when concrete mathematics or the source contradicts the target;
- report `structural_blocked` with target-specific capability evidence when the obstruction belongs
  to a shared, earlier, multi-file, statement-level, or external capability package;
- report `validation_inconsistency` when attached tools and coordinator evidence disagree;
- return an unchanged durable blocker immediately when no relevant source or interface changed.

On a retry, state the materially new premise, API, or strategy before using proof tools. If none
exists, do not repeat searches or failed approaches.

## Proof workflow

1. Inspect the assigned declaration, local context, source passage, and directly relevant earlier
   APIs. Confirm signatures before use.
2. Prefer an existing result, focused rewriting, a standard constructor/equivalence, then a
   lower-level construction. Before writing the main proof, decompose its mathematical argument into
   a short sequence of natural intermediate lemmas. Search for existing declarations that express
   each step; when none exists, introduce a focused helper lemma with a meaningful mathematical
   statement.
3. Keep the final theorem proof small and structural: it should primarily instantiate, rewrite with,
   and compose those lemmas. Avoid giant tactic blocks, deeply nested reasoning, or monolithic term
   proofs. If a proof becomes difficult to read or debug, extract its substantive intermediate steps
   into named lemmas.
4. Prefer reusable source-level lemmas when an intermediate result captures genuine mathematics used
   elsewhere; otherwise use a focused private helper near the theorem. Do not extract arbitrary
   one-line tactic fragments or create helpers that merely restate the goal.
5. Preserve and validate independent progress. Remove speculative edits, unused helpers, and
   abandoned imports.
6. After editing, prepare affected dependencies once and request fresh diagnostics in import order.

## Constraints

- Do not change a public declaration's interface. Report a defective statement for focused review.
- Do not add placeholders, axioms, unsafe declarations, circular helpers, warning suppression,
  heartbeat workarounds, umbrella imports, or `aesop`.
- Treat proof decomposition as part of correctness and maintainability. A proof that typechecks but
  remains an unnecessarily large monolith should be refactored into natural lemmas before completion.
- Keep imports focused and chronological. Leave holes outside the assignment unchanged.
- Propose a package capability only after checking two concrete alternatives.

## Report

Set `complete=true` only when every assigned hole is gone and its span is diagnostic-clean. Set
`disposition` to:

- `proved` for complete work;
- `partial` when checked edits remain but assigned holes remain;
- `retryable` only when a named materially new strategy remains;
- `statement_defect`, `structural_blocked`, or `validation_inconsistency` for terminal routes.

Use `failed_attempts` only for new evidence and `blocker_refs` for unchanged durable evidence. Every
failed attempt includes `capability`: use `null` for local work, or a package proposal containing a
stable `capability_key`, `owner_kind`, exact `owner_paths`, and `needed_result` for structural work.
Return the structured report once files are stable. `statement_defect` and `structural_blocked`
require target-specific `failed_attempts`; `partial` requires retained scoped edits.
