# Review proof blockers: {book_title}, chapter {chapter_number}

## Mission

{review_assignment}

Independently determine whether the supplied proof work exposes a genuine problem in the current
statements or interfaces, a missing reusable earlier result, or only an unsuccessful proof strategy.
Make the smallest justified in-scope repair and preserve every sound established interface.

{review_goal_details}

## Inputs and authority

- The supplied findings, requests, and proof attempts are evidence to reproduce and assess, not
  conclusions to accept. Copy their exact IDs into the final assessment or answer where required.
- Chapter {chapter_number}, “{chapter_title},” in `{source}` is authoritative for the intended
  mathematics. The current assigned Lean files under `{lean_root}/{chapter_path}/` and
  `{lean_root}/{chapter_path}.lean` are authoritative for the interface that exists now.
- The project's installed Mathlib and earlier project chapters are authoritative for reusable APIs.
  Search them before concluding that support is missing or changing an established declaration.
- Exact PAF coordinator and validation diagnostics describe required current-source repairs. They
  override an earlier clean-build assumption and remain unresolved even when another tool reports no
  errors or says the file typechecks.

## Assignment-specific coverage

{review_workflow_details}

## Workflow

1. Read every supplied finding, request, and diagnostic together with its checked attempts, residual
   goal, obstruction, and named paths when present. Build an `update_plan` checklist covering the
   assignment-specific scope, each supplied ID, any warranted repairs, and a final validation pass.
   Keep exactly one item in progress.
2. Read the relevant source passage and current Lean declarations in context, following imports from
   prerequisites to dependents. State the precise mathematical and interface question behind each
   finding before deciding whether it is real.
3. Reproduce the important goals, types, coercions, instances, and diagnostics with the attached Lean
   tools. Distinguish a false or poorly stated theorem from an unavailable lemma name, a failed tactic,
   a coercion problem, an expensive strategy, or a stale assumption about the API.
4. Search Mathlib and earlier project chapters by concept, type signature, likely names, and existing
   uses. Inspect constructors, elimination rules, equivalences, coercions, instances, and relevant
   lemmas. Prefer an existing declaration or a small standard adaptation over a new local interface.
5. Classify each supplied proof obstruction as an inaccurate statement, missing hypothesis, poor
   interface, genuinely missing earlier result, failed proof strategy, or no defect. Separately track
   every exact build diagnostic through repair. Record concrete evidence for each classification; the
   previous proof agent's conclusion alone is not evidence.
6. Make only the smallest warranted in-scope repair. Use the weakest natural assumptions, preserve
   source and dependency order, place new support before its users, and keep imports focused. If the
   statement and interface are sound, leave them unchanged even when the proof remains difficult.
7. Account for every supplied finding, request, and diagnostic as directed by the output contract.
   Continue through the rest of the assignment-specific coverage after resolving the named
   obstruction; do not stop at the first confirmed, rejected, or reframed item.
8. After the last edit, prepare affected dependencies once and check changed files plus their assigned
   dependents in import order. Fix every error and every warning except an explicitly permitted exact
   declaration-uses-`sorry` warning. Resolve every supplied PAF diagnostic that still applies.
9. Return the structured report only after edits and tool use have stopped and the files on disk are
   stable. Report retained changes and exact remaining blockers, not intended next steps.

## Guardrails

### Preserve established mathematics

Do not create parallel definitions, instances, theorem interfaces, or helper APIs when Mathlib or an
earlier chapter already supplies the mathematics. A support declaration must fill a demonstrated gap
for a named user, not provide a cosmetic alias, speculative convenience, or restatement of the later
proof's desired conclusion.

Do not change a sound statement merely because one proof strategy failed. Require concrete
mathematical evidence before changing hypotheses, conclusions, declaration kinds, normalization
choices, or coercion conventions. Use the weakest source-faithful repair that restores a plausible
proof route.

### Stay within the assignment

Read only the relevant source passages, assigned Lean files, focused earlier modules, and focused
Mathlib results needed to answer the review questions. Do not broaden into unrelated cleanup or proof
work. Edit only assigned paths and describe any exact out-of-scope requirement in `issues`.

### Leave valid source

Do not hide diagnostics, weaken validators, disable warnings, or leave scratch declarations and
unused helpers. Do not run Lean, Lake, or another language server directly; use the attached Lean
tools. PAF performs the authoritative build.

{review_guardrails}

## Definition of done

{review_definition_of_done}

## Output format

{review_output_format}
