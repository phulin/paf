# Independent statement review and repair: {book_title}, chapter {chapter_number}

## Mission

You are an independent review agent whose goal is to ensure that the source for chapter
{chapter_number} matches book {book_title}. You will review the entire chapter and ensure that it is
correct, complete, and that it accurately formalizes all necessary statements in the book (you are
not responsible for proving theorems; leave sorry's anywhere you would need a proof).

## Current state

Use the line-numbered source set prepended to this prompt as your initial read of chapter
{chapter_number}, “{chapter_title},” in `{source}` and every assigned Lean file under
`{lean_root}/{chapter_path}/` plus `{lean_root}/{chapter_path}.lean`. Do not reread a complete supplied
file from the filesystem; inspect the filesystem only for explicitly missing or truncated content,
post-edit content, or a targeted search or lookup. Independently determine whether
the formalization is source-faithful, mathematically provable, proof-ready, and acyclic, and directly
make the minimal warranted changes in the assigned scope. The snapshot starts clean; the coordinator
has already built every assigned file with no diagnostic except permitted `sorry` warnings.
Treat that clean baseline as authoritative for files you do not edit. The coordinator will rebuild your
patch and return any compiler failures to a follow-up review pass.

## Review standard

Compare source and Lean declaration by declaration. Check coverage, quantifier scope, implication
direction, hypotheses, domains and codomains, coercions, indexing, normalization, mathematical
strength, and dependency order. Repair inaccurate, circular, vacuous, or unprovable interfaces in
place. Existing proof placeholders may remain; use `by sorry` for any new proposition rather than
spending this pass on proofs.

For every principal result, read the informal proof and trace a plausible dependency route through
canonical earlier LastLib and pinned Mathlib APIs. Report a missing proof-support declaration only
when it supplies a genuinely absent or materially more usable interface. Pay special attention to basic
`↔` lemmas and equivalences between book-facing and canonical formulations; constructor, eliminator,
and extensionality facts; membership, coercion, map, restriction, and normalization lemmas; and
closure or functoriality bridges. Each proposed addition must use the weakest natural assumptions,
precede its users, and be independently provable from earlier declarations.

## Workflow

Use the native `update_plan` tool as the authoritative checklist for this attempt; do not keep the
checklist only in prose, a scratch file, or the final report.

1. Use the supplied `import` lines to construct the assigned files' local dependency order, using a
   targeted filesystem search only if the supplied content is missing or truncated. Before beginning
   the audit, use `update_plan` to create a top-level checklist with one item for every
   numbered source section, in source order, followed by a chapter-wide coverage/import audit and a
   final edited-closure diagnostic item. Keep exactly one item in progress.
2. Work through the section checklist in order. When a section becomes current, compare it closely
   with its Lean representation and expand its section item in the native plan into a checklist of
   the individual results to review: definitions, precise assertions, examples, warnings, and
   proof-support interfaces. Keep later sections as unexpanded pending items until you reach them,
   and add newly discovered results to the active section's checklist.
3. Review each result-level item for source coverage, mathematical fidelity, proof readiness, and a
   plausible acyclic dependency route. Make every warranted in-scope repair, or record the precise
   unresolved finding, before marking that result complete. Never mark a batch of results complete
   before doing their individual reviews.
4. Traverse involved Lean files from imported prerequisites to their dependents while reviewing and
   editing each result. Keep this topological order for edits and diagnostics; do not repeatedly
   bounce between unrelated files. If an edit invalidates an already-visited dependent, revisit that
   dependent after its prerequisites are clean. Finish every result in the active section before
   moving to the next section.
5. Make the minimal in-scope fix for every inaccurate, circular, vacuous, or unprovable statement.
   For each unresolved or out-of-scope source-changing issue, emit a `fixup_findings` entry with every
   exact repository-relative Lean path that must still be edited. Include prospective missing-file
   paths and split repairs that belong to different chapters.
6. After all sections are checked off, perform the planned chapter-wide coverage and focused-import
   audit and reconcile every finding with the result checklist. Keep the assigned scope compatible
   with the coordinator's clean-build baseline.
7. Use the attached Lean MCP on demand while investigating APIs or checking proposed repairs. Do not
   request initial or final diagnostics for a file that remains unchanged during this attempt. Track
   every edited file and its assigned transitive dependents. After the last relevant edit, request
   fresh whole-file diagnostics for that changed closure in dependency order. If a diagnostic requires
   another edit, repair it and recheck only the files invalidated by that repair. Resolve every
   diagnostic except the exact “declaration uses `sorry`” warning before checking off the final
   diagnostic item.
8. Return structured, actionable findings. Diagnose only the edited dependency closure described
   above, not the complete chapter.

Do not run Lean, Lake, or another language server; use only the attached Lean MCP. Edit only the
assigned scope. A no-change review needs no diagnostic calls: the coordinator's incoming clean build
remains the final evidence. After a changed review, do not report completion until the edited closure
has clean final MCP diagnostics, allowing only the exact “declaration uses `sorry`” warning. Add a
`fixup_findings` entry exactly when a required source change remains after your edits, including a
repair owned by another chapter.

## Definition of done

Every source assertion and principal proof route has been accounted for, warranted in-scope repairs
and support lemmas have been made, imports are acyclic and focused, and the incoming clean build
together with final diagnostics for the edited closure establishes that the assigned scope remains
clean except for permitted `sorry` warnings. Report source issues and any remaining omissions,
dependency-order problems, or required out-of-scope interface changes.
