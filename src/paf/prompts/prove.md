# Prove assigned declarations: {book_title}, chapter {chapter_number}

## Mission

Replace exactly the assigned proof holes with Lean-checked proofs. Preserve established public
interfaces and unrelated holes. The Lean statement is the proof target; `{source}` is the default
mathematical argument, not authority for a stronger claim.

## First decision

Before broad search, classify the assignment from its current source and handoff:

- proceed when an exact lemma, focused proof route, or materially new retry strategy exists;
- report `statement_defect` when concrete mathematics or the source contradicts the target;
- report `upstream_blocked` with an `upstream_requests` entry when an earlier owner must add an API;
- report `validation_inconsistency` when attached tools and coordinator evidence disagree;
- return an unchanged durable blocker immediately when no relevant source or interface changed.

On a retry, state the materially new premise, API, or strategy before using proof tools. If none
exists, do not repeat searches or failed approaches.

## Proof workflow

1. Inspect the assigned declaration, local context, source passage, and directly relevant earlier
   APIs. Confirm signatures before use.
2. Prefer an existing result, focused rewriting, a standard constructor/equivalence, then a
   lower-level construction. Use focused private helpers when useful.
3. Preserve and validate independent progress. Remove speculative edits, unused helpers, and
   abandoned imports.
4. After editing, prepare affected dependencies once and request fresh diagnostics in import order.

## Constraints

- Do not change a public declaration's interface. Report a defective statement for focused review.
- Do not add placeholders, axioms, unsafe declarations, circular helpers, warning suppression,
  heartbeat workarounds, umbrella imports, or `aesop`.
- Keep imports focused and chronological. Leave holes outside the assignment unchanged.
- Request an earlier-chapter result only after checking two concrete alternatives.

## Report

Set `complete=true` only when every assigned hole is gone and its span is diagnostic-clean. Set
`disposition` to:

- `proved` for complete work;
- `partial` when checked edits remain but assigned holes remain;
- `retryable` only when a named materially new strategy remains;
- `statement_defect`, `upstream_blocked`, or `validation_inconsistency` for those terminal routes.

Use `failed_attempts` only for new evidence, `blocker_refs` for unchanged durable evidence, and
`upstream_requests` only for an earlier owner. Return the structured report once files are stable.
`statement_defect` requires target-specific `failed_attempts`; `upstream_blocked` requires a valid
`upstream_requests` owner handoff; and `partial` requires retained scoped edits.
