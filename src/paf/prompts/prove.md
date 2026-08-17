# Prove theorems: {book_title}, chapter {chapter_number}

## Goal

Replace as many `sorry` or `admit` placeholders as possible with proofs that Lean checks. Spend the
attempt constructing, testing, and improving proofs. A difficult proof is the work of this stage, not
by itself a reason to stop.

Do not change theorem statements or existing definitions to make proofs easier. You may change proof
bodies, add focused imports, and add fully proved helper lemmas when they are genuinely needed.

## Workflow

1. Find every unresolved placeholder in the assigned files and order those files from prerequisites
   to dependents using their imports. Use `update_plan` to create one checklist item for each file that
   contains work, in that order, followed by a final diagnostic check. Keep exactly one item in
   progress.
2. For the active file, add the unresolved declaration names to its checklist item. Read each
   declaration, its surrounding code, and the relevant part of chapter {chapter_number} in `{source}`.
3. Use the book's informal proof as the default mathematical plan. Search earlier project chapters
   and the project's Mathlib version for the definitions and lemmas that carry out each step. Search
   by concept and type signature, inspect existing uses, and confirm exact theorem signatures before
   relying on them.
4. Choose a tractable placeholder or one needed by later proofs. Try a concrete proof term or tactic,
   inspect the resulting goals, and iterate. Prefer, in order, direct computation, an exact earlier
   theorem, focused rewriting or simplification, a standard constructor or equivalence, and only then
   lower-level implementation details.
5. When the final result does not follow directly, prove smaller intermediate facts. Add a local,
   private, or reusable helper only when existing APIs do not already provide it. Every new helper must
   be proved, used, and placed at the earliest valid point in the assigned files.
6. Stay with a plausible proof through several meaningfully different checked attempts. Search for a
   different earlier result, unfold the local definition, prove a focused helper, construct the object
   directly, or change the tactic structure. One guessed name or failed tactic is not enough reason to
   stop.
7. If repeated concrete attempts expose the same obstruction, preserve every clean proof and useful
   helper, record the exact remaining goal and attempts, and continue with independent placeholders.
8. Complete each file before moving to the next. After all files have been visited, check every edited
   file and the assigned files that depend on it. Remove or revert any speculative edit that does not
   pass diagnostics; every retained proof and helper must be accepted by Lean.

## Guardrails

Existing declaration interfaces are fixed during this stage. Do not change declaration kinds, names,
namespaces, arguments, hypotheses, result types, attributes, section behavior, or the bodies of
existing definitions, structures, and instances.

Do not add a helper merely because it would be convenient. First search Mathlib and earlier chapters,
and reuse an existing result whenever it provides the required mathematics. Do not add placeholders,
axioms, unsafe declarations, or unused scaffolding.

Do not increase Lean's maximum heartbeat limit or disable heartbeat limits to make a proof pass. If
a proof exceeds the current limit, find a less computationally intensive strategy: break the argument
into focused lemmas, reuse stronger existing results, reduce unnecessary unfolding or search, or
restructure the proof so Lean can check each step efficiently.

Report a problem with a statement only when you have concrete mathematical evidence that it is false,
does not match the book, or cannot follow from its assumptions. A failed search, unknown theorem name,
tactic failure, coercion error, timeout, or unfinished proof is not enough evidence.

If an assigned statement really must change, leave it unchanged and record the evidence with the
failed attempt. If the proof needs a specific reusable result that belongs in an earlier chapter and
cannot be added here, use `upstream_requests`. Include the blocked
declaration, file, remaining Lean goal, smallest needed result, proposed earlier owner and paths, and
at least two meaningfully different attempts. Continue proving independent declarations.

## Definition of done

All placeholders that this attempt can prove have been replaced by Lean-checked proofs. For each
remaining proof, report its checked attempts and exact residual goal. Set `complete` to `true` only
when no placeholder remains.

## Output format

Return the structured report once, after tool use and edits have stopped. It must describe the stable
files on disk, not planned work. Use only these fields:

- `changed`: `true` exactly when an allowed edit remains.
- `complete`: `true` only when no placeholder remains.
- `summary`: when files changed, concise past-tense prose naming the proved declarations and important
  helpers, suitable for a commit body; otherwise, why no edit was retained.
- `issues`: tooling, diagnostic, or out-of-scope problems that are not individual proof attempts;
  otherwise an empty list.
- `source_issues`: genuine defects in the informal textbook; otherwise an empty list. Each entry must
  give `location`, an exact identifying `source_excerpt`, a mathematical `description`, and the
  smallest `suggested_correction`. Do not use this field for an ordinary failed proof or missing Lean
  interface.
- `failed_attempts`: every unresolved assigned proof; otherwise an empty list. Each entry must give
  its repository-relative `path`, fully qualified `declaration`, at least two meaningfully different
  checked `attempts`, exact `remaining_goal`, and concrete `obstruction`. State suspected statement or
  interface problems as evidence, not conclusions; PAF sends these entries to an independent review.
- `upstream_requests`: only missing reusable results that belong in an earlier chapter; otherwise an
  empty list. Each entry must give `blocked_declaration`, `consumer_path`, `residual_goal`, the smallest
  `needed_result`, `owner_chapter_id`, exact `owner_paths`, and at least two
  `attempted_alternatives`.
