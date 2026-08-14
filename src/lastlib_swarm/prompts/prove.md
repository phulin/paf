# Proof implementation: {book_title}, chapter {chapter_number}

## Mission

Replace every mathematically sound `sorry` or `admit` placeholder you can reach with a
kernel-checked proof. This is an implementation task, not a chapter audit: spend the attempt
constructing and testing proofs, not merely inventorying obstacles. Be tenacious. A difficult proof
is the work of this stage, not by itself a reason to leave the placeholder unresolved.

## Immutable constraints

Existing statements are immutable during this pass. Do not change declaration kinds, names,
namespaces, binders, hypotheses, result types, attributes, or section behavior to make a proof easier.
Focused imports required by a valid proof are permitted under the common import policy.

## Workflow

1. Locate the unresolved placeholders from the supplied assigned source and use the supplied imports
   to order their files from prerequisites to dependents. Before editing a proof, use the native
   `update_plan` tool to create an authoritative checklist with one item for every file that contains work
   for this attempt, in that dependency order, followed by a final edited-closure diagnostic item.
   Keep exactly one file in progress and do not keep this checklist only in prose or a scratch file.
2. Work through the file checklist in order. For the active file, record its unresolved declaration
   names in the plan item, then read each proof's surrounding declaration and the relevant part of
   chapter {chapter_number} in `{source}`. Do not read the complete book or unrelated assigned files
   unless a target genuinely requires that context. If proof work requires an additional assigned
   helper file, add it to the checklist at its dependency-correct position before editing it.
3. Use the informal chapter's proof as the default mathematical plan. Translate its intermediate
   constructions and reductions into Lean whenever they are sound and compatible with pinned APIs.
   Depart materially from that plan only when Lean's library structure makes another proof
   substantially clearer or the informal argument omits a necessary step; record the reason.
4. Within the active file, choose a tractable or prerequisite placeholder and work on it concretely.
   Confirm exact theorem signatures from source, try candidate terms or tactics, inspect the
   resulting goal, and iterate.
5. Prove intermediate facts when the final result does not yield directly. You may add focused
   local or private helper lemmas in the assigned files, derive a reusable missing lemma at the
   earliest chronologically valid location in scope, and refactor the target proof around those
   stepping stones. Every added helper must itself be proved and used; do not add new placeholders.
6. Keep every clean proof and mathematically reusable helper even when another placeholder remains.
   Do not create cosmetic edits or unused scaffolding solely to register a change.
7. Stay with a plausible target through elaboration failures and incomplete proof states. Inspect
   the exact residual goals, search declarations and existing uses, simplify the target into helper
   lemmas, and try several materially different proof shapes. A guessed theorem name, a failed
   tactic, or one unsuccessful approach is only evidence for the next experiment.
8. Give up on a target only after sustained concrete effort with multiple checked experiments that
   expose the same hard obstruction, or when you have a specific mathematical argument that the
   statement is false or cannot follow from its assumptions. When one target has genuinely reached
   that point, preserve the useful work and continue to independent placeholders.
9. Mark the active file complete only after every placeholder in it is either proved or has received
   the sustained concrete attempt and precise reporting required below. Then advance to the next
   file; never check off files out of order or batch-complete unvisited files.
10. After every file is checked off, diagnose edited files and affected dependents as needed and
    check off the final diagnostic item. Do not spend time running final diagnostics on untouched
    files, whose incoming build is already certified clean.

Prefer, in order, definitional equality, an exact earlier theorem, focused rewriting or
simplification, a canonical constructor or equivalence, and only then unfolded infrastructure.

## Genuine statement obstructions

If a target is inaccurate or cannot follow from its assumptions, leave its statement and placeholder
unchanged and report a precise statement/API fixup request in `fixup_findings`, including every exact
repository-relative Lean path that must be edited. Likewise, if sustained proof work establishes
that the proof genuinely requires a missing earlier LastLib interface which cannot be added within
scope, request the minimal interface addition there instead of repeatedly reporting "no pinned API"
as an ordinary issue. A failed search, guessed theorem name, tactic failure, coercion error, timeout,
unfinished proof, or difficulty finding a library lemma alone is not evidence that the statement
needs fixup or that the proof is impossible.

## Definition of done

All provable placeholders have been replaced by kernel-checked proofs. Every remaining placeholder is
identified as either an ordinary unresolved proof or a concrete statement/interface obstruction in
`fixup_findings`. For each ordinary unresolved proof, add an `issues` entry containing its exact path
and declaration, the several materially different candidate terms, tactics, helper lemmas, or
reductions tried in this attempt, and the residual goal or recurring obstruction. A thin attempt does
not become acceptable merely by documenting it. Never put a required source edit in `issues`. Set
`complete` to `true` only when no placeholder remains.
