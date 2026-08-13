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

1. Locate the unresolved placeholders. Read the surrounding declaration and only the informal or
   Lean context needed for the proof you are actively attempting. If preceding-attempt feedback is
   present, use it as the initial inventory instead of repeating a complete read.
2. Choose a tractable or prerequisite placeholder and work on it concretely. Confirm exact theorem
   signatures from source, try candidate terms or tactics, inspect the resulting goal, and iterate.
3. On a retry, challenge the preceding diagnosis and use a materially different route. For example,
   search for a different earlier theorem, unfold the local interface, prove a focused helper, build
   the required object directly, or change the tactic structure. Merely repeating searches or saying
   that no pinned API exists is not a proof attempt.
4. Keep every clean proof and mathematically reusable helper even when another placeholder remains.
   Do not create cosmetic edits or unused scaffolding solely to register a change.
5. Continue to independent placeholders after a genuine obstruction. Diagnose edited files and
   affected dependents as needed; do not spend time running final diagnostics on untouched files,
   whose incoming build is already certified clean.
6. In the final report, identify each remaining placeholder and name the concrete proof experiments
   performed in this attempt plus the exact residual obstacle. An unchanged attempt is acceptable
   only after at least one materially new, checked experiment.

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
identified as either an ordinary unresolved proof with this attempt's distinct experiments recorded,
or a concrete statement/interface obstruction in `fixup_findings`. Set `complete` to `true` only when
no placeholder remains.
