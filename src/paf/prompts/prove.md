# Prove assigned declarations: {book_title}, chapter {chapter_number}

## Mission

Replace exactly the assigned proof holes with Lean-checked proofs. Preserve established public
interfaces and unrelated holes. The Lean statement is the proof target; `{source}` is the default
mathematical argument, not authority for a stronger claim.

## First decision

Before broad search, classify the assignment from its current source and handoff:

- proceed when an exact lemma, focused proof route, or materially new retry strategy exists;
- report `statement_defect` when concrete mathematics or the source contradicts the target;
- report `structural_blocked` with target-specific evidence when the obstruction may come from a
  missing, wrong, or too-weak earlier interface rather than the assigned proof;
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
- Flag a possible upstream problem only after checking two concrete alternatives. State what the
  consumer needs and which earlier paths should be inspected; do not decide ownership yourself.

## Report

Set `complete=true` only when every assigned hole is gone and its span is diagnostic-clean. Set
`disposition` to:

- `proved` for complete work;
- `partial` when checked edits remain but assigned holes remain;
- `retryable` only when a named materially new strategy remains;
- `statement_defect`, `structural_blocked`, or `validation_inconsistency` for terminal routes.

Use `failed_attempts` only for new evidence and `blocker_refs` for unchanged durable evidence. Every
failed attempt includes the legacy-named `capability` field: use `null` for local work, or use it as
an upstream-review hypothesis containing a stable issue key in `capability_key`, `owner_kind`, exact
suspected `owner_paths`, and the consumer's `needed_result`. This is evidence for a tandem review,
not a package or an ownership decision.
Return the structured report once files are stable. `statement_defect` and `structural_blocked`
require target-specific `failed_attempts`; `partial` requires retained scoped edits.
