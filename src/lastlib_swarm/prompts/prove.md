# Whole-chapter proof pass: {book_title}, chapter {chapter_number}

## Mission

Read the full informal chapter in `{source}` and every assigned Lean file under
`{lean_root}/{chapter_path}/` plus `{lean_root}/{chapter_path}.lean`. Prove every mathematically sound
`sorry` or `admit` that follows from its existing statement, earlier declarations, and pinned APIs.

## Immutable constraints

Statements are immutable during this pass. Do not change declaration kinds, names, namespaces,
binders, hypotheses, result types, attributes, or section behavior to make a proof easier. Focused
imports required by a valid proof are permitted under the common import policy.

## Workflow

1. Inventory every placeholder and read the complete assigned file set before editing.
2. Confirm exact theorem signatures and APIs from source rather than memory.
3. Make one coherent proof-writing pass over the entire assigned file set. Attempt every sound
   placeholder once without stopping to diagnose each speculative proof.
4. After that whole-file pass, request whole-file diagnostics for every assigned file. Cluster the
   failures, then iterate only over proofs and dependent declarations that fail, using goals, batched
   tactic attempts, code actions, declaration lookup, and fresh diagnostics.
5. Continue past hard declarations so one obstruction does not block independent later proofs.
6. Finish with fresh whole-file diagnostics and account for every remaining placeholder and message.

Prefer, in order, definitional equality, an exact earlier theorem, focused rewriting or
simplification, a canonical constructor or equivalence, and only then unfolded infrastructure.

## Genuine statement obstructions

If a target is inaccurate or cannot follow from its assumptions, leave its statement and placeholder
unchanged and report a precise statement/API fixup request in `fixup_findings`, including every exact
repository-relative Lean path that must be edited. A failed search, guessed theorem name, tactic
failure, coercion error, timeout, or unfinished proof is not evidence that the statement needs fixup.

## Definition of done

All provable placeholders have been replaced by kernel-checked proofs. Every remaining placeholder is
identified as either an ordinary unresolved proof or a concrete statement/interface obstruction, and
the assigned files have fresh diagnostics with no unexplained messages. Set `complete` to `true` only
when no placeholder remains.
