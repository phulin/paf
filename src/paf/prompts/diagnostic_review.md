# Repair coordinator diagnostics: {book_title}, chapter {chapter_number}

## Mission

{diagnostic_trigger}

Repair every supplied diagnostic at its cause, making only the smallest changes to the declarations,
imports, or definitions needed to clear it. This is targeted diagnostic repair, not a failed proof
attempt or a full-chapter statement review.

It is not your job to complete proofs, but do not delete prior proof work. If a proof no longer
completes after your changes, finish it with `sorry`. If a proof no longer compiles, move the relevant
failing steps into a comment labeled as a prior attempt before replacing them with `sorry`. When a
supplied diagnostic is inside a proof body, repair that body as narrowly as possible; do not revisit
unrelated statements or try to prove unassigned theorems.

Deliver the complete assigned Lean scope with zero errors and zero non-`sorry` warnings. The only
permitted warnings are exact declaration-uses-`sorry` warnings for allowed proof placeholders. This
requirement applies whether or not you changed any files. Set `complete` to `true` only when this
delivery state has been established.

## Inputs and authority

- Exact PAF coordinator and validation diagnostics describe required current-source repairs. They
  override an earlier clean-build assumption and remain unresolved even when another tool reports no
  errors or says the file typechecks.
- The current assigned Lean files under `{lean_root}/{chapter_path}/` and
  `{lean_root}/{chapter_path}.lean` are authoritative for the interface that exists now.
- Chapter {chapter_number}, “{chapter_title},” in `{source}` is authoritative for the intended
  mathematics when a diagnostic requires inspecting or repairing a statement.
- The project's installed Mathlib and earlier project chapters are authoritative for reusable APIs.
  Search them before adding support or changing an established declaration.

## Workflow

1. Read every supplied diagnostic and its named path. Build an `update_plan` checklist covering each
   diagnostic, its source owner, any required repair, and a final validation pass. Keep exactly one
   item in progress.
2. Trace secondary diagnostics to their earliest cause and group diagnostics with the same cause.
   Read only the relevant source passages and Lean declarations needed to understand and repair that
   cause.
3. Reproduce the diagnostics with the attached Lean tools. A supplied PAF warning remains required
   work even if a later tool response reports no errors, says the file typechecks, or exposes warning
   bodies only as a count.
4. Make the smallest in-scope repair. Preserve sound statements and interfaces, use focused imports,
   and do not broaden into unrelated cleanup or proof work.
5. After the last edit, prepare affected dependencies once and check changed files plus every assigned
   dependent in import order. Fix every error and every warning except an explicitly permitted exact
   declaration-uses-`sorry` warning.
6. Return the structured report only after edits and tool use have stopped and the files on disk are
   stable. Report retained changes and exact remaining blockers, not intended next steps.

## Guardrails

Do not return a no-change report while a supplied diagnostic still applies. If a diagnostic belongs
to an out-of-scope dependency, leave local source unchanged and identify the exact owner path in
`issues`. Do not add placeholders merely to silence an error or warning except as directed above for
a proof broken by your changes.

Do not hide diagnostics, weaken validators, disable warnings or linters, or leave scratch
declarations and unused helpers. Do not run Lean, Lake, or another language server directly; use the
attached Lean tools. PAF performs the authoritative build.

Edit only assigned paths. Do not broaden into unrelated source review or proof work.

## Definition of done

Every supplied diagnostic has been removed at its cause, the complete assigned Lean scope and its
affected assigned dependents have zero errors and zero non-`sorry` warnings, no unrelated interface
was changed, and any genuinely out-of-scope owner is identified precisely.

## Output format

Return the structured report once, after tool use and edits have stopped. It must describe the stable
files on disk, not planned work. Use only these fields:

- `complete`: `true` only when every supplied in-scope diagnostic is resolved and the definition of
  done is met.
- `summary`: if edits remain, concise past-tense prose naming the repaired diagnostics and their
  causes, suitable for a commit body; otherwise, why no edit was retained.
- `issues`: precise remaining diagnostic, tooling, or out-of-scope blockers; otherwise an empty list.
- `source_issues`: genuine defects in the informal textbook; otherwise an empty list. Each entry must
  give `location`, an exact identifying `source_excerpt`, a mathematical `description`, and the
  smallest `suggested_correction`.
