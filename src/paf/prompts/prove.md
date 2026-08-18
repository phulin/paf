# Prove assigned theorems: {book_title}, chapter {chapter_number}

## Mission

Complete exactly the proof declarations assigned by PAF. Normally they are listed in a bounded proof
chunk; if no chunk list is present, the assignment is every unresolved declaration in the allowed
files. Replace every assigned `sorry` or `admit` with a proof that Lean checks, preserve every sound
proof already present, and leave the affected files acceptable to PAF's stricter validation build.

This is a proof-construction stage. Spend the attempt developing, testing, and improving proofs; a
difficult goal is the work, not by itself a reason to stop. The theorem statements and established
interfaces have already passed review and are fixed here.

## Inputs and authority

- When the PAF requirements contain an assigned-proof-chunk list, that list is the exact work scope
  and other unresolved declarations are reserved for later chunks. Without such a list, every
  unresolved declaration in the allowed files is assigned.
- The relevant passage of chapter {chapter_number} in `{source}` supplies the default mathematical
  argument. The current assigned Lean files supply the exact statements and local interfaces.
- Mathlib and earlier project chapters supply reusable definitions and results. Confirm exact
  signatures in the project's installed versions before relying on them.
- Retry handoffs contain prior checked attempts, durable blocker IDs, and PAF validation diagnostics.
  Prior diagnoses are evidence to improve; exact coordinator diagnostics are required repair work.

## Workflow

1. Determine the exact assigned declarations from the PAF requirements and locate them in the current
   files. Order the targets from prerequisites to dependents. Use `update_plan` to create one
   checklist item per assigned declaration or tightly coupled group, followed by a final validation
   item. Keep exactly one item in progress.
2. For the active target, read its statement, surrounding definitions, available local lemmas, and
   the corresponding informal proof in `{source}`. Write down the mathematical route and the exact
   Lean objects, equalities, equivalences, coercions, or instances each step requires.
3. Search Mathlib and earlier project chapters by concept and type signature as well as likely names.
   Inspect theorem statements and existing uses. Prefer a direct established theorem or a small
   standard adaptation over unfolding implementation details or creating a new helper.
4. Try a concrete proof term or focused tactic sequence and inspect the resulting goals. Prefer, in
   order, direct computation, an exact earlier theorem, focused rewriting or simplification, a
   standard constructor or equivalence, and only then lower-level construction.
5. Iterate on checked evidence. When an approach fails, use the residual goal to change one material
   part of the strategy: choose another theorem, expose a relevant definition, prove an intermediate
   fact, construct the object directly, or reorganize the tactic structure. One guessed declaration
   name, tactic failure, coercion error, or timeout is not enough reason to stop.
6. Add a local, private, or reusable helper only after the search shows that no existing result
   supplies the needed step. Every helper must state useful mathematics, be fully proved, be used by
   an assigned target, and appear at the earliest valid point. Keep any added import focused and
   chronological.
7. Finish all tractable targets in the assignment and preserve clean partial progress. Remove
   speculative edits, unused helpers, and abandoned imports. If a target remains blocked after several
   meaningfully different checked attempts, capture its exact residual goal and obstruction, then
   continue with independent assigned targets.
8. After the last edit, prepare affected dependencies once and check every edited file plus assigned
   dependents in import order. Fix every error and every warning except the exact warning that a
   declaration uses `sorry`. An error-free elaboration or “typecheck clean” message does not clear a
   remaining warning from PAF's authoritative build.
9. On a retry, begin with every supplied PAF validation diagnostic and resolve each one that still
   applies before returning. If a durable blocker is unchanged, put its ID in `blocker_refs` instead
   of copying its evidence into a new failed attempt.
10. Return the structured report only after edits and tool use have stopped and the files on disk are
    stable. Report what was actually retained, not intended next steps.

## Guardrails

### Fixed interfaces and proof scope

Do not change declaration kinds, names, namespaces, arguments, hypotheses, result types, attributes,
section behavior, or the bodies of existing definitions, structures, and instances. Work only on
the declarations assigned to this attempt, apart from focused imports, fully proved helpers they
need, and repairs demanded by supplied validation diagnostics. Do not prove, rewrite, or report
failures for placeholders reserved for later chunks.

### Proof integrity

Do not add `sorry`, `admit`, axioms, unsafe declarations, `sorryAx`, artificial contradictions, or
unused scaffolding. Do not weaken a statement, smuggle the conclusion into a hypothesis, or hide a
circular proof in a helper. Every retained declaration must be accepted by Lean for the intended
mathematical reason.

### Resource discipline

Do not increase or disable heartbeat limits to force a proof through. Break expensive arguments into
focused lemmas, reuse stronger results, reduce unnecessary unfolding or automation, and structure
the proof so Lean can check each step efficiently.

### Escalation threshold

Report a statement or interface defect only with concrete mathematical evidence that the statement
is false, mismatches the book, or cannot follow from its assumptions. If an assigned interface really
must change, leave it unchanged and record the evidence in `failed_attempts`. If the smallest missing
reusable result naturally belongs to an earlier chapter, submit an `upstream_requests` entry naming
the consumer, residual goal, needed result, proposed owner and paths, and at least two checked
alternatives. Continue proving independent assigned targets.

## Definition of done

The proof assignment is done only when every assigned placeholder has been replaced by a
Lean-checked proof and PAF validation has no error or non-`sorry` warning. Set `complete` to `true`
exactly in that case. Unassigned placeholders reserved for later work do not make this report
incomplete.

If an assigned target remains unresolved, preserve all independent clean progress, set `complete` to
`false`, and report new checked evidence for that target or reference its unchanged durable blocker.

## Output format

Return the structured report once, after tool use and edits have stopped. It must describe the stable
files on disk, not planned work. Use only these fields:

- `complete`: `true` exactly when no assigned placeholder or non-`sorry` diagnostic remains.
- `summary`: if edits remain, concise past-tense prose naming the proved declarations and important
  helpers, suitable for a commit body; otherwise, why no edit was retained.
- `issues`: tooling, diagnostic, or out-of-scope problems that are not individual proof attempts;
  otherwise an empty list.
- `source_issues`: genuine defects in the informal textbook; otherwise an empty list. Each entry must
  give `location`, an exact identifying `source_excerpt`, a mathematical `description`, and the
  smallest `suggested_correction`. Do not use this field for an ordinary failed proof or missing Lean
  interface.
- `failed_attempts`: only new or materially changed unresolved assigned proofs; otherwise an empty
  list. Each entry must give its repository-relative `path`, fully qualified `declaration`, at least
  two meaningfully different checked `attempts`, exact `remaining_goal`, concrete `obstruction`, and
  a `disposition`: `retry`, `missing_upstream`, `statement_review`, `interface_review`, or
  `genuine_blocker`. Use either review disposition only with concrete mathematical evidence; PAF
  waits for a repeated fingerprint before escalating it.
- `blocker_refs`: durable blocker IDs from the handoff that are unchanged in this attempt; otherwise
  an empty list. Never copy their full evidence into the report again.
- `upstream_requests`: only missing reusable results that belong in an earlier chapter; otherwise an
  empty list. Each entry must give `blocked_declaration`, `consumer_path`, `residual_goal`, the smallest
  `needed_result`, `owner_chapter_id`, exact `owner_paths`, and at least two
  `attempted_alternatives`.
