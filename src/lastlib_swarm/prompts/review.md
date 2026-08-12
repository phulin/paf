# Independent statement review and repair: {book_title}, chapter {chapter_number}

## Mission

Read chapter {chapter_number}, “{chapter_title},” in `{source}` and every assigned Lean file under
`{lean_root}/{chapter_path}/` plus `{lean_root}/{chapter_path}.lean`. Independently determine whether
the formalization is source-faithful, mathematically provable, proof-ready, and acyclic, and directly
make the minimal warranted changes in the assigned scope. The snapshot starts clean; the coordinator
will rebuild your patch after you return clean LSP diagnostics and route any compiler-only failures
through fixup.

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

1. Read the assigned files' `import` lines, construct their local dependency order, and traverse the
   files from imported prerequisites to their dependents. Keep this topological order for the audit,
   edits, and final diagnostic pass; do not repeatedly bounce between unrelated files. If an edit
   invalidates an already-visited dependent, revisit that dependent after its prerequisites are clean.
2. Audit source coverage and the mathematical fidelity of every declaration.
3. Audit proof readiness, dependency routes, and missing reusable interfaces.
4. Make the minimal in-scope fix for every inaccurate, circular, vacuous, or unprovable statement.
   For each unresolved or out-of-scope source-changing issue, emit a `fixup_findings` entry with every
   exact repository-relative Lean path that must still be edited. Include prospective missing-file
   paths and split repairs that belong to different chapters.
5. Keep the assigned scope compatible with the coordinator's clean-build baseline.
6. Audit imports in every file reviewed or changed against the common focused-import policy.
7. Use the attached Lean MCP to request whole-file diagnostics for every assigned Lean file. After
   each edit, request fresh diagnostics for the edited file and every assigned dependent that may be
   affected. Resolve every diagnostic except the exact “declaration uses `sorry`” warning before
   finishing.
8. Recheck the complete chapter in dependency order and return structured, actionable findings.

Do not run Lean, Lake, or another language server; use only the attached Lean MCP. Edit only the
assigned scope. Do not report completion until all assigned files have clean final MCP diagnostics,
allowing only the exact “declaration uses `sorry`” warning. Add a `fixup_findings` entry exactly when
a required source change remains after your edits, including a repair owned by another chapter.

## Definition of done

Every source assertion and principal proof route has been accounted for, warranted in-scope repairs
and support lemmas have been made, imports are acyclic and focused, and final whole-file MCP
diagnostics are clean throughout the assigned scope except for permitted `sorry` warnings. Report
source issues and any remaining omissions, dependency-order problems, or required out-of-scope
interface changes.
