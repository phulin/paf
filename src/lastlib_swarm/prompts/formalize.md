# Formalize statements: {book_title}, chapter {chapter_number}

## Mission

Read chapter {chapter_number}, “{chapter_title},” in `{source}` from its numbered heading through the
next heading of the same level. Create or update `{lean_root}/{chapter_path}/` and its aggregator
`{lean_root}/{chapter_path}.lean` so the chapter exposes an accurate, proof-ready Lean API in source
order.

This is a single optimistic drafting pass. Proofs may use `by sorry`; definitions should have genuine
bodies whenever the canonical construction is clear. The draft need not elaborate yet. Do not
translate motivation, history, proof narration, or redundant paraphrases into declarations.

## Coverage and proof readiness

Represent every precise assertion: labeled declarations, displayed identities and diagrams, exact
sequences, compatibility results, hypotheses embedded in prose, examples, and mathematically precise
warnings. Read informal proofs for dependency planning even though their narration is not itself
formalized.

For every principal result, trace a plausible route through earlier project declarations and pinned
Mathlib. Add genuinely missing intermediate interfaces, especially basic `↔` lemmas; equivalences
between book-facing and canonical formulations; constructor, eliminator, and extensionality facts;
membership, coercion, map, restriction, and normalization lemmas; and closure or functoriality
bridges. Search for a canonical declaration first. A new bridge must use the weakest natural
assumptions, precede its users, and be independently provable from earlier material.

Match finiteness, separation, completeness, characteristic, normalization, and typeclass assumptions
exactly. For a genuine false or underspecified source assertion, make only the minimal principled
correction and record the defect in the structured `source_issues` ledger rather than leaving a
source-issue comment in Lean. If an unavailable earlier result is essential, add only a clearly
marked, mathematically natural local dependency guess—not one engineered to imply the desired
conclusion.

## Workflow

Use the native `update_plan` tool as the authoritative checklist for this attempt; do not keep the
checklist only in prose, a scratch file, or the final report.

1. Inventory the complete source chapter and the existing assigned Lean files. Before drafting any
   declaration, use `update_plan` to create a top-level checklist with one item for every
   numbered source section, in source order, followed by a chapter-wide coverage and proof-readiness
   audit. Keep exactly one item in progress.
2. Work through that section checklist in order. When a section becomes current, inspect it closely
   and expand its section item in the native plan into a checklist of the individual results to
   formalize: definitions, precise assertions, examples, warnings, and any proof-support interfaces
   the section requires. Keep later sections as unexpanded pending items until you reach them, and
   add newly discovered results to the active section's checklist instead of silently absorbing
   them into a broad task.
3. For each result-level item, inspect canonical earlier LastLib and pinned Mathlib interfaces, then
   create or repair its declarations. Mark that item complete as soon as the result is accurately
   represented or explicitly accounted for; never mark a batch of results complete before doing
   their individual work.
4. Finish every result in the active section before moving to the next section. Preserve source and
   dependency order while completing the statement and definition pass.
5. After all sections are checked off, perform the planned chapter-wide audit, add only genuinely
   missing reusable bridges, and record unresolved dependency guesses and API conflicts for the
   later fixup pass. Check off the audit only after reconciling its findings with the result
   checklist.

Do not run Lean, Lake, or a language server, and do not request LSP diagnostics. The coordinator will
reconcile all chapter drafts in the repeated global fixup pass.

## Definition of done

Every substantive source assertion is represented or explicitly accounted for, declarations follow
source order, and definitions have real bodies where practical. Report coverage gaps, dependency
guesses, source issues, and important provisional interface choices. Compiler cleanliness is not a
condition of this pass. Set `complete` to `true` only when the full chapter coverage pass is finished;
the coordinator rejects an incomplete draft rather than silently treating it as formalized.
