# Independent statement review and repair: {book_title}, chapter {chapter_number}

## Mission

Read chapter {chapter_number}, “{chapter_title},” in `{source}` and every assigned Lean file under
`{lean_root}/{chapter_path}/` plus `{lean_root}/{chapter_path}.lean`. Independently determine whether
the formalization is source-faithful, mathematically provable, proof-ready, and acyclic, and directly
make the minimal warranted changes in the assigned scope. The snapshot starts clean; the coordinator
will rebuild your patch and route any resulting compiler diagnostics through fixup.

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

1. Audit source coverage and the mathematical fidelity of every declaration.
2. Audit proof readiness, dependency routes, and missing reusable interfaces.
3. Make the minimal in-scope fix for every inaccurate, circular, vacuous, or unprovable statement.
   For each unresolved or out-of-scope source-changing issue, emit a `fixup_findings` entry with every
   exact repository-relative Lean path that must still be edited. Include prospective missing-file
   paths and split repairs that belong to different chapters.
4. Keep the assigned scope compatible with the coordinator's clean-build baseline.
5. Audit imports in every file reviewed or changed against the common focused-import policy.
6. Recheck the complete chapter and return structured, actionable findings.

Do not run Lean, Lake, or a language server, and do not request LSP diagnostics. Edit only the
assigned scope. Set `needs_fixup` to `true` exactly when a required source change remains after your
edits, including a repair owned by another chapter.

## Definition of done

Every source assertion and principal proof route has been accounted for, warranted in-scope repairs
and support lemmas have been made, and imports are acyclic and focused. Report source issues and any
remaining omissions, dependency-order problems, or required out-of-scope interface changes.
