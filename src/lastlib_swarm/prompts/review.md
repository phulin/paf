# Independent statement review: {book_title}, chapter {chapter_number}

## Mission

Read chapter {chapter_number}, “{chapter_title},” in `{source}` and every assigned Lean file under
`{lean_root}/{chapter_path}/` plus `{lean_root}/{chapter_path}.lean`. Independently determine whether
the formalization is source-faithful, mathematically provable, proof-ready, and acyclic.

## Review standard

Compare source and Lean declaration by declaration. Check coverage, quantifier scope, implication
direction, hypotheses, domains and codomains, coercions, indexing, normalization, mathematical
strength, and dependency order. Correct inaccurate, circular, vacuous, or unprovable interfaces
minimally; do not weaken correct mathematics merely because its proof is difficult. Existing proof
placeholders may remain.

For every principal result, read the informal proof and trace a plausible dependency route through
canonical earlier LastLib and pinned Mathlib APIs. Add a missing proof-support declaration only when
it supplies a genuinely absent or materially more usable interface. Pay special attention to basic
`↔` lemmas and equivalences between book-facing and canonical formulations; constructor, eliminator,
and extensionality facts; membership, coercion, map, restriction, and normalization lemmas; and
closure or functoriality bridges. Each addition must use the weakest natural assumptions, precede its
users, and be independently provable from earlier declarations.

## Workflow

1. Audit source coverage and the mathematical fidelity of every declaration.
2. Audit proof readiness, dependency routes, and missing reusable interfaces.
3. Repair inaccurate, circular, vacuous, or unprovable statements minimally.
4. Obtain clean whole-file diagnostics for the assigned scope.
5. Audit imports in every file reviewed or changed against the common focused-import policy.
6. Recheck the complete chapter. If it is already faithful and diagnostic-clean, make no changes;
   that no-change pass is the review fixed-point signal.

## Definition of done

Every source assertion and principal proof route has been accounted for, all repairs and added support
lemmas are justified, imports are acyclic and focused, and fresh diagnostics contain no unexpected
messages. Report omissions, source issues, dependency-order problems, and all interface changes.
