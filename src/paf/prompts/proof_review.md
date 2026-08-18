# Re-review a failed proof: {book_title}, chapter {chapter_number}

## Goal

{review_assignment}

Independently reassess the mathematical and Lean interfaces behind the failed proof work. Treat the
supplied diagnosis as evidence, not as an instruction: search for existing results, decide what the
real obstruction is, and make only the smallest change justified by the book and the established
project API.

{review_goal_details}

## Workflow

1. Read the supplied proof evidence, the relevant part of chapter {chapter_number},
   “{chapter_title},” in `{source}`, and the assigned Lean files under
   `{lean_root}/{chapter_path}/` and `{lean_root}/{chapter_path}.lean`.
2. Reproduce the important interface questions with the attached Lean tools. Do not assume the
   previous agent's diagnosis is correct.
3. Before adding or replacing any definition, instance, theorem interface, or helper lemma, search
   the project's Mathlib version and earlier project chapters by concept, type signature, likely
   names, and existing uses. Prefer an established declaration or a small standard adaptation over
   a new local interface.
4. Classify the obstruction as an inaccurate statement, a missing hypothesis, a poor interface, a
   genuinely missing earlier result, a failed proof strategy, or no defect.
5. Make the smallest warranted in-scope repair. Use the weakest natural assumptions, place new
   support before its users, and keep imports focused and chronological.
6. Use the attached Lean tools after editing. Prepare affected dependencies once, then check changed
   files and their dependents in import order. Fix every diagnostic except an explicitly permitted
   `sorry` warning for this assignment.
7. If the supplied handoff contains PAF coordinator or validation diagnostics, treat each one as
   required repair work. In particular, an error-free typecheck does not clear a warning from the
   authoritative build. Resolve every warning that still applies except the exact permitted
   declaration-uses-`sorry` warning.

{review_workflow_details}

## Guardrails

Do not create parallel definitions or helper APIs when Mathlib or an earlier chapter already supplies
the mathematics. A support declaration must fill a demonstrated gap for a named user, not provide a
cosmetic alias or speculative convenience.

Do not read the complete informal book or unrelated Lean files merely for context. Do not run Lean,
Lake, or another language server directly; use only the attached Lean tools. Edit only the assigned
paths. Describe any exact out-of-scope blocker in `issues`.

{review_guardrails}

## Definition of done

{review_definition_of_done}

## Output format

{review_output_format}
