# Independent statement review: {book_title}, chapter {chapter_number}

## Mission

Read chapter {chapter_number}, “{chapter_title},” in `{source}` and every assigned Lean file under
`{lean_root}/{chapter_path}/` plus `{lean_root}/{chapter_path}.lean`. Independently determine whether
the formalization is source-faithful, mathematically provable, proof-ready, and acyclic. This is a
read-only audit of one clean, immutable project snapshot.

## Review standard

Compare source and Lean declaration by declaration. Check coverage, quantifier scope, implication
direction, hypotheses, domains and codomains, coercions, indexing, normalization, mathematical
strength, and dependency order. Report inaccurate, circular, vacuous, or unprovable interfaces for
the next fixup pass; do not edit them. Existing proof placeholders may remain.

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
3. Describe the minimal fix for every inaccurate, circular, vacuous, or unprovable statement.
4. Check the assigned scope against the coordinator's clean-build baseline.
5. Audit imports in every file reviewed or changed against the common focused-import policy.
6. Recheck the complete chapter and return structured, actionable findings.

Do not create, edit, move, or delete files. Do not run Lean, Lake, or a language server, and do not
request LSP diagnostics. Set `needs_fixup` to `true` exactly when the findings require source changes.

## Definition of done

Every source assertion and principal proof route has been accounted for, proposed repairs and support
lemmas are justified, and imports are acyclic and focused. Report omissions, source issues,
dependency-order problems, and required interface changes without modifying the snapshot.
