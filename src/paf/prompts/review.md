# Independent statement review and repair: {book_title}, chapter {chapter_number}

## Goal

Independently verify that the assigned Lean files accurately and completely represent chapter
{chapter_number}, “{chapter_title},” from {book_title}. Repair every statement, definition, import, or
supporting interface that is inaccurate or unusable. This stage reviews the formalization; it does
not prove theorem bodies, so existing proof placeholders may remain and new proposition proofs may
use `by sorry`.

PAF has already built the incoming files successfully, allowing only warnings for declarations that
use `sorry`.

Deliver the complete assigned Lean scope with zero errors and zero non-`sorry` warnings. The only
permitted warnings are exact declaration-uses-`sorry` warnings for allowed proof placeholders. This
requirement applies whether or not you changed any files. Set `complete` to `true` only when this
delivery state has been established.

## Workflow

1. Read the chapter dynamically from its numbered heading through the next heading of the same level
   in `{source}`. Read every assigned Lean file under `{lean_root}/{chapter_path}/` and the top-level
   file `{lean_root}/{chapter_path}.lean`, following their imports from prerequisites to dependents.
2. Before deciding that a definition or interface is missing, search the project's Mathlib version
   and earlier project chapters. Search by concept and type signature as well as likely names; inspect
   constructors, elimination rules, equivalences, coercions, instances, lemmas, and existing uses.
   Prefer the established API over a new local version.
3. Use `update_plan` to make one checklist item for every numbered source section, in source order,
   followed by a chapter-wide coverage/import check and a final diagnostic check. If you are assigned
   only one section, break the section into a checklist of its logical subcomponents. Keep exactly one
   item in progress.
4. For each checklist item, name the definitions, precise assertions, examples, hypotheses, warnings,
   and proof-support results it requires. Compare them with the Lean declarations one by one.
5. Check mathematical meaning as well as surface similarity: quantifiers, implication direction,
   hypotheses, domains and codomains, coercions, indexing, normalization, strength, and dependency
   order must all match. Read each main theorem's informal proof far enough to confirm that its
   statement has a plausible route through existing earlier results.
6. Make the smallest in-scope repair for every inaccurate, circular, vacuous, unnecessarily strong,
   or otherwise unprovable interface. Add a new supporting declaration only after the searches in
   step 2 show that no suitable declaration already exists.
7. After all sections are complete, perform the planned chapter-wide coverage and import check. Do
   not defer a repair that belongs in the assigned files; describe any exact out-of-scope blocker in
   `issues`.
8. Use the attached Lean tools as needed while investigating. After the final edit, check the edited
   files and every assigned file that depends on them, from prerequisites to dependents. Fix every
   warning and error except the exact warning that a declaration uses `sorry`.
9. Return a concise report of what you checked, what you changed, and any precise work that remains.

## Guardrails

Do not reinvent definitions, structures, instances, notation, or theorem interfaces that already
exist in Mathlib or earlier chapters. A new wrapper or bridge must solve a demonstrated mismatch for
a named declaration in this chapter; it must not be a cosmetic alias or a speculative convenience.
Use the weakest natural assumptions, place it before its users, and ensure that it is provable from
earlier material.

Do not read the complete informal book or unrelated Lean files merely for context; use focused
searches as questions arise. Do not run Lean, Lake, or another language server directly. Use only
the attached Lean tools. Edit only the assigned paths. A no-change review needs no diagnostic calls
because the incoming build is already clean.

## Definition of done

Every source assertion and every main theorem's likely proof route has been checked. Existing Mathlib
and earlier-chapter APIs have been reused wherever possible, all warranted in-scope repairs have been
made, imports remain focused and cycle-free, and the complete assigned Lean scope has zero errors and
zero warnings except permitted exact declaration-uses-`sorry` warnings. Report source problems and
any exact out-of-scope changes still required.

## Output format

Return the structured report once, after tool use and edits have stopped. It must describe the stable
files on disk, not planned work. Use only these fields:

- `complete`: `true` only when the definition of done is met.
- `summary`: if edits remain, concise past-tense prose naming the main files or declarations and the
  purpose of the edits, suitable for a commit body; otherwise, why no edit was needed.
- `issues`: precise remaining statement, interface, diagnostic, tooling, or out-of-scope blockers;
  otherwise an empty list.
- `source_issues`: genuine defects in the informal textbook; otherwise an empty list. Each entry must
  give `location`, an exact identifying `source_excerpt`, a mathematical `description`, and the
  smallest `suggested_correction`. Do not use this field for a Lean API gap or proof failure. A source
  issue is not a reason to stop; make the smallest principled accommodation and continue elsewhere.
