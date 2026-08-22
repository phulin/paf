# Prove assigned declarations: {book_title}, chapter {chapter_number}

## Mission

Replace every assigned proof hole with a Lean-checked proof. Preserve established public interfaces
and holes outside the assignment. The current Lean statement is the target; the corresponding
passage of `{source}` supplies the intended mathematical argument.

## Proof workflow

1. Read the assigned declaration, its local context, and the relevant source passage. Identify the
   mathematical steps needed for the proof.
2. Inspect directly relevant earlier project APIs and Mathlib declarations, confirming exact
   signatures before use.
3. Try the natural high-level proof first: an existing theorem, focused rewriting, a standard
   constructor or equivalence, or a short composition of these.
4. When that route does not close the goal, state the missing intermediate claims and prove them as
   focused private helpers. Failure to find a convenient existing lemma is not evidence that the
   target needs an interface change.
5. Use Lean goals and small checked probes to guide each revision. Preserve independently checked
   progress, but remove speculative edits, unused helpers, and abandoned imports.
6. After editing, prepare affected dependencies once and request fresh diagnostics in import order.

Prefer readable, mathematically meaningful decomposition for a genuinely long proof. A direct proof
is also acceptable when it is clear and maintainable; do not create helper lemmas merely to satisfy a
preferred shape.

## When a proof remains unresolved

Do not stop merely because library search or the first tactic strategy failed. First attempt the
mathematical construction available from the target's hypotheses, including focused local
intermediate lemmas where appropriate.

If sustained checked work still leaves a goal:

- record the exact residual goal reproduced by Lean;
- record each materially different strategy with the concrete probe or proof fragment used and its
  observed result;
- explain the mathematical obstruction, distinguishing a local proof failure from evidence that the
  statement is inaccurate or that a result may belong in an earlier module;
- for a suspected statement problem, give the source comparison, counterexample, or precise
  contradiction;
- for a suspected earlier-module gap, explain why the needed result cannot reasonably be established
  as a private local helper and name the exact earlier paths and result that should be evaluated.

These are observations for the coordinator to assess, not a decision that proof work is terminal.
The number of attempts is not itself evidence: report substantive, Lean-checked routes rather than
padding the report with superficial searches.

## Constraints

- Do not change a public declaration's interface. Report concrete evidence when the statement itself
  appears inaccurate.
- Do not add placeholders, axioms, unsafe declarations, circular helpers, warning suppression,
  heartbeat workarounds, umbrella imports, or `aesop`.
- Keep imports focused and chronological. Leave holes outside the assignment unchanged.
- Do not treat an unavailable lemma name, failed tactic, coercion mismatch, or expensive strategy as
  evidence of a statement or interface problem without investigating the underlying mathematics.

## Completion and report

Set `complete=true` and `disposition=proved` only when every assigned hole is gone and its span is
diagnostic-clean. Otherwise set `complete=false` and `disposition=incomplete`, retain any independent
checked progress, and describe every remaining target in `unresolved_proofs`. Use
`validation_inconsistency` only when attached Lean tools and current coordinator diagnostics disagree.

For each unresolved proof, choose an evidence `kind`:

- `local_proof_failure` when the statement still appears sound but the checked work did not finish;
- `suspected_statement_defect` only with concrete mathematical or source evidence;
- `suspected_upstream_gap` only with a precise `upstream_hypothesis`; otherwise set
  `upstream_hypothesis` to `null`.

Use `blocker_refs` only when an exact supplied blocker still applies and the handoff contains no new
source, interface, reviewer guidance, or viable proof route. Return the structured report only after
files and tool use are stable.
