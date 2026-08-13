# Proof implementation: {book_title}, chapter {chapter_number}

## Mission

Replace as many mathematically sound `sorry` or `admit` placeholders as possible with
kernel-checked proofs. This is an implementation task, not a chapter audit: spend the attempt
constructing and testing proofs, not merely inventorying obstacles.

## Immutable constraints

Statements are immutable during this pass. Do not change declaration kinds, names, namespaces,
binders, hypotheses, result types, attributes, or section behavior to make a proof easier. Focused
imports required by a valid proof are permitted under the common import policy.

## Workflow

1. Locate the unresolved placeholders. For the proof you are actively attempting, read its
   surrounding declaration and the relevant part of chapter {chapter_number} in `{source}`. Do not
   read the complete book or assigned file set unless the target genuinely requires that context.
2. Choose a tractable or prerequisite placeholder and work on it concretely. Confirm exact theorem
   signatures from source, try candidate terms or tactics, inspect the resulting goal, and iterate.
3. Keep every clean proof and mathematically reusable helper even when another placeholder remains.
   Do not create cosmetic edits or unused scaffolding solely to register a change.
4. Continue to independent placeholders after a genuine obstruction. Diagnose edited files and
   affected dependents as needed; do not spend time running final diagnostics on untouched files,
   whose incoming build is already certified clean.
5. An unchanged attempt is acceptable only after at least one concrete, checked proof experiment.

Prefer, in order, definitional equality, an exact earlier theorem, focused rewriting or
simplification, a canonical constructor or equivalence, and only then unfolded infrastructure.

## Genuine statement obstructions

If a target is inaccurate or cannot follow from its assumptions, leave its statement and placeholder
unchanged and report a precise statement/API fixup request in `fixup_findings`, including every exact
repository-relative Lean path that must be edited. Likewise, if the proof genuinely requires a
missing earlier LastLib interface, request the minimal interface addition there instead of repeatedly
reporting "no pinned API" as an ordinary issue. A failed search, guessed theorem name, tactic failure,
coercion error, timeout, or unfinished proof alone is not evidence that the statement needs fixup.

## Definition of done

All provable placeholders have been replaced by kernel-checked proofs. Every remaining placeholder is
identified as either an ordinary unresolved proof or a concrete statement/interface obstruction in
`fixup_findings`. For each ordinary unresolved proof, add an `issues` entry containing its exact path
and declaration, the candidate terms, tactics, or lemmas tried in this attempt, and the residual goal
or failure. Never put a required source edit in `issues`. Set `complete` to `true` only when no
placeholder remains.
