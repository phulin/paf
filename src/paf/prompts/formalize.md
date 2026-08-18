# Formalize statements: {book_title}, chapter {chapter_number}

## Goal

Turn every precise mathematical statement in chapter {chapter_number}, “{chapter_title},” into an
accurate, usable Lean declaration. Every chapter must have its own chapter-specific top-level file
`{lean_root}/{chapter_path}.lean`; create or update it and any chapter files under
`{lean_root}/{chapter_path}/` that it imports. Do not leave this chapter's content only in a shared
file or a file belonging to another chapter.

This stage builds the chapter's definitions and theorem statements; it does not prove the theorems.
Theorem proofs may be `by sorry`, but every definition must have a real body when the construction is
clear, and every file must be free of errors and warnings other than Lean's exact warning for a
declaration that uses `sorry`.

## Workflow

1. Read the complete chapter in `{source}`, from its numbered heading through the next heading of the
   same level, together with the existing assigned Lean files. Use `update_plan` to make one checklist
   item for each numbered source section, in source order, followed by a final chapter-wide coverage
   and diagnostic check. Keep exactly one item in progress.
2. Before creating a definition, notation, instance, theorem interface, or helper lemma, search the
   project's Mathlib version and earlier project chapters for an existing version. Search by concept
   and type signature, not only by a guessed name, and inspect constructors, elimination rules,
   equivalences, coercions, instances, and existing uses. Reuse the established declaration whenever
   it expresses the required mathematics.
3. Work through the source sections in order. Expand the active checklist item to name every precise
   definition, assertion, displayed identity or diagram, exact sequence, example, hypothesis, and
   mathematically meaningful warning in that section.
4. Create or repair the Lean declarations for the active section. Account explicitly for an assertion
   that needs no separate theorem because it follows immediately from a definition or is already
   covered by a stronger, source-faithful result.
5. For each main theorem, read its informal proof and identify a plausible route through declarations
   that already exist in Mathlib or earlier chapters. Add a new supporting interface only when that
   search shows a real gap and the new declaration has a named user in this chapter.
6. Preserve source order and dependency order. No file belonging to this chapter may import a future
   chapter in this book. An introduction chapter must contain only its own mathematical content and
   likewise must not import any future chapter in this book. Finish and check the active section
   before moving to the next one. After all sections are complete, perform the planned chapter-wide
   coverage and import check.
7. Use the attached Lean tools after editing. Prepare the affected dependent files once, then request
   whole-file diagnostics from prerequisites to dependents. Fix every error and every warning except
   the exact warning that a declaration uses `sorry`. Replace time-consuming proposition proofs with
   `by sorry`; proving them belongs to the prove stage.
8. If PAF returns build diagnostics for another attempt, address every finding that still applies and
   repeat the final focused diagnostic check.

## Guardrails

Do not reinvent mathematics or local infrastructure that Mathlib or an earlier chapter already
provides. A differently named existing declaration is still the preferred choice when a small,
standard adaptation makes it usable. Do not introduce a parallel definition merely because its
surface syntax resembles the book more closely. Add a bridge lemma or equivalence only when the
canonical API cannot express a needed book-facing statement directly; use the weakest natural
assumptions, place it before its users, and make sure it can be proved from earlier material.

Represent all substantive mathematical content, but do not translate motivation, history, proof
narration, or redundant paraphrases into declarations. Match hypotheses, types, coercions,
normalization conventions, and assumptions such as finiteness, separation, completeness, and
characteristic exactly.

Keep chapter ownership and import direction explicit: create the file for this specific chapter, and
never add a forward import—an import of a later chapter in this book—from one of its files. In
particular, an introduction file contains only the introduction's mathematical content; it must not
serve as an import hub for future chapters in this book or import them to make declarations
available. This forward-import rule concerns only chapter order within this book; it does not by
itself prohibit imports from Mathlib or other external libraries.

If the source contains a genuinely false or underspecified assertion, make only the smallest
mathematically principled correction and record the problem in `source_issues`. If an essential
earlier fact is missing, add only a natural dependency that belongs at that earlier point—never an
assumption designed simply to imply the desired conclusion.

Do not run Lean, Lake, or another language server directly. Use the attached Lean tools; PAF performs
the authoritative build after each attempt.

## Definition of done

Every substantive source assertion is represented or explicitly accounted for. Existing Mathlib and
earlier-chapter APIs have been reused wherever possible, new interfaces fill demonstrated gaps, the
declarations follow source and dependency order, definitions have real bodies where practical, and
the chapter has its own file with no imports of future chapters in this book, and diagnostics are
clean except for declarations using `sorry`. Introduction files contain only their chapter's
mathematical content and do not import future chapters in this book. Report remaining coverage gaps,
source problems, and important interface choices. Set `complete` to `true` only after the full
coverage/import check and the clean diagnostic check are finished.

## Output format

Return the structured report once, after tool use and edits have stopped. It must describe the stable
files on disk, not planned work. Use only these fields:

- `complete`: `true` only when the definition of done is met.
- `summary`: if edits remain, concise past-tense prose naming the main files or declarations and the
  purpose of the edits, suitable for a commit body; otherwise, why no edit was needed.
- `issues`: precise remaining coverage, Lean-interface, diagnostic, tooling, or out-of-scope blockers;
  otherwise an empty list.
- `source_issues`: genuine defects in the informal textbook; otherwise an empty list. Each entry must
  give `location`, an exact identifying `source_excerpt`, a mathematical `description`, and the
  smallest `suggested_correction`. Do not use this field for a Lean API gap or proof failure. A source
  issue is not a reason to stop; make the smallest principled accommodation and continue elsewhere.
