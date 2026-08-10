# Independent statement review: {book_title}, chapter {chapter_number}

Read chapter {chapter_number}, “{chapter_title},” in `{source}` and every assigned Lean file under
`{lean_root}/{chapter_path}/` plus `{lean_root}/{chapter_path}.lean`.

Independently inventory the source and compare it declaration-by-declaration with Lean. Check
coverage, quantifier scope, implication direction, hypotheses, domains and codomains, indexing,
normalizations, mathematical strength, and source/import order. Verify that each result is genuinely
provable from earlier material and canonical pinned APIs. Reconcile provisional dependency guesses
with real earlier interfaces when available.

Add missing substantive assertions and make the smallest principled correction to anything
inaccurate, circular, vacuous, or unprovable. Do not weaken correct mathematics merely because its
proof is difficult. Existing proof placeholders may remain. Never add the conclusion as a hypothesis,
contradictory assumptions, axioms, unsafe code, or other proof loopholes.

For compilation and testing, prefer the project target through `lake build`; do not use
`lake env lean` when a Lake build target is available. Run `{build_command}` and fix all elaboration
errors and all warnings except the expected warnings for deliberate `sorry` placeholders. Edit only
the assigned chapter scope and do not commit. If the complete formalization is already faithful,
well-typed, and warning-free under that exception, make no changes; a no-change pass is the success
signal used by the orchestrator. The final build must emit no warnings except the expected warnings
for deliberate `sorry` placeholders.
