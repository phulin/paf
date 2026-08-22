# Prove assigned declarations: {book_title}, chapter {chapter_number}

## Mission

Replace every assigned proof hole with a Lean-checked proof. Preserve established public interfaces
and holes outside the assignment. The current Lean statement is the target; the corresponding
passage of `{source}` supplies the intended mathematical argument.

## Proof workflow

1. Order the assigned declarations by dependency and work on the earliest foundational target first.
   Do not prefer an easy downstream theorem merely because it can use an unresolved assigned or
   reserved declaration.
2. Read the assigned declaration, its local context, and the relevant source passage. Identify the
   mathematical steps needed for the proof. When the source supplies a proof roadmap, treat those
   steps as the default implementation plan.
3. Inspect directly relevant earlier project APIs and Mathlib declarations, confirming exact
   signatures before use.
4. Try the natural high-level proof first: an existing theorem, focused rewriting, a standard
   constructor or equivalence, or a short composition of these.
5. When that route does not close the goal, state the missing intermediate claims and prove them as
   focused helpers in the editable chapter. Helpers may be public when they record reusable
   intermediate mathematics. Failure to find a convenient existing lemma means that this local
   lemma chain must be implemented; it is not evidence that the target needs an interface change.
6. Use Lean goals and small checked probes to guide each revision. Preserve independently checked
   progress, but remove speculative edits, unused helpers, and abandoned imports.
7. After editing, prepare affected dependencies once and request fresh diagnostics in import order.

Prefer readable, mathematically meaningful decomposition for a genuinely long proof. A direct proof
is also acceptable when it is clear and maintainable; do not create helper lemmas merely to satisfy a
preferred shape.

## When a proof remains unresolved

Do not stop merely because library search or the first tactic strategy failed. First attempt the
mathematical construction available from the target's hypotheses, including focused local
intermediate lemmas where appropriate.

An assigned theorem is responsible for the intermediate mathematics needed to prove it. Missing
lemmas, constructions, or APIs in the assigned file or editable chapter are local proof work, not
upstream gaps. A source-roadmap step without an existing API must be formalized locally. Before
reporting failure, make a checked attempt at each major source-roadmap step; searches and probes that
only reproduce the original goal do not establish a blocker.

If sustained checked work still leaves a goal:

- record the exact residual goal reproduced by Lean;
- record each materially different strategy with the concrete probe or proof fragment used and its
  observed result;
- explain the mathematical obstruction, distinguishing a local proof failure from evidence that the
  statement is inaccurate or that a result may belong in an earlier module;
- for a suspected statement problem, give the source comparison, counterexample, or precise
  contradiction;
- for a suspected earlier-module gap, explain why the needed result cannot reasonably be established
  as a local helper and name the exact strictly earlier, non-editable paths and result that should be
  evaluated. An upstream owner in the current file or editable chapter is invalid.

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
- `suspected_local_interface_defect` when a definition, signature, instance, notation, import, or
  other interface owned by the editable work unit appears defective. Give concrete Lean evidence;
  this requests same-unit review and must set `upstream_hypothesis` to `null`;
- `suspected_upstream_gap` only with a precise `upstream_hypothesis` owned by a strictly earlier
  module outside the editable scope; otherwise use `local_proof_failure` and set
  `upstream_hypothesis` to `null`. A hypothesis whose `owner_paths` include the blocked file or any
  editable path will be rejected by the coordinator.

Prior blocker reports are untrusted hypotheses, not established facts, and their repetition count is
not evidence. If a handoff says machinery in the editable scope is missing, reinterpret it as a local
implementation plan. Use `blocker_refs` only when an exact supplied blocker still applies, identifies
a strictly earlier non-editable owner, and the handoff contains no new source, interface, reviewer
guidance, or viable proof route. Return the structured report only after files and tool use are stable.
