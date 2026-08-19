# Clean Lean warnings: {book_title}, chapter {chapter_number}

## Mission

The Lean build completed successfully and produced usable artifacts, but PAF recorded the supplied
non-`sorry` warnings as local cleanup obligations. Resolve every supplied warning at its cause with
the smallest possible source change.

Preserve all existing declarations, theorem statements, interfaces, and proof terms. In particular,
do not replace, simplify, reorganize, or re-prove an existing proof merely because another proof
would also work. Touch a proof body only when a supplied warning originates inside it, and then make
the narrowest local edit that removes that warning while retaining the surrounding proof structure.

This is warning cleanup, not formalization, statement review, failed-proof repair, or an opportunity
for general refactoring. Do not work on unrelated placeholders or warnings that were not supplied.

## Workflow

1. Make an `update_plan` checklist containing every supplied warning and a final validation pass.
2. Reproduce each warning with the attached Lean tools and identify its exact source location.
3. Apply the smallest in-scope correction. Prefer deletion of an unused tactic or binding, a focused
   syntax update, or another local edit over restructuring a declaration or proof.
4. Prepare affected dependencies once after the final edit, then inspect diagnostics for each changed
   file and assigned dependent in import order.
5. Finish only when all supplied warnings are gone, no new error or non-`sorry` warning exists in the
   assigned scope, and existing proofs have otherwise been left undisturbed.

## Guardrails

Do not change public interfaces, mathematical statements, hypotheses, declaration names, or imports
unless a supplied warning cannot be fixed without the import edit. Do not add or replace proofs with
`sorry`, `admit`, or another placeholder. Do not disable warnings or linters, weaken validators, add
suppression attributes, or leave scratch declarations. Do not run Lean, Lake, or another language
server directly; use the attached Lean tools. Edit only assigned paths.

If a warning belongs to an out-of-scope dependency, leave local sources unchanged and report the
exact owner path in `issues`.

## Output format

Return the structured report only after edits and tool use have stopped:

- `complete`: `true` only when every supplied warning is resolved and final diagnostics are clean.
- `summary`: concise past-tense prose naming the warnings removed and the minimal retained edits.
- `issues`: exact remaining warning, tooling, or out-of-scope blockers; otherwise an empty list.
- `source_issues`: genuine defects in the informal textbook; normally an empty list. Each entry must
  give `location`, an exact identifying `source_excerpt`, a mathematical `description`, and the
  smallest `suggested_correction`.
