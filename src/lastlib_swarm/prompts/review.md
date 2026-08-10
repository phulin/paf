# Independent statement review: {book_title}, chapter {chapter_number}

Read chapter {chapter_number}, “{chapter_title},” in `{source}` and every assigned Lean file under
`{lean_root}/{chapter_path}/` plus `{lean_root}/{chapter_path}.lean`.

Independently inventory the source and compare it declaration-by-declaration with Lean. Check
coverage, quantifier scope, implication direction, hypotheses, domains and codomains, indexing,
normalizations, mathematical strength, and source/import order. Verify that each result is genuinely
provable from earlier material and canonical pinned APIs. Reconcile provisional dependency guesses
with real earlier interfaces when available.

Also perform a proof-readiness audit, not only a source-coverage audit. For every principal result,
read the informal proof or proof sketch, locate the exact earlier project and Mathlib APIs, and trace
a plausible proof dependency route. Add every genuine intermediate declaration that route requires
when it is missing, even if the prose treats it as obvious or mentions it only inside a proof. Check
especially for basic `↔` lemmas and equivalences between book-facing and canonical Lean
formulations; constructor/eliminator and extensionality facts; membership, coercion, map,
restriction, and normalization lemmas; closure and functoriality results; and short bridges between
successive proof steps. Do not leave every proof agent to recreate a missing chapter API locally.

Search for a canonical theorem first. A new proof-support lemma must provide a genuinely missing or
materially more usable interface, use the weakest natural assumptions, occur before its users, and
be independently provable from earlier declarations. Reject helpers that restate the target, assume
its conclusion, depend on later results, or otherwise conceal circularity. Preserve accurate support
lemmas merely omitted from the informal exposition.

Add missing substantive assertions and make the smallest principled correction to anything
inaccurate, circular, vacuous, or unprovable. Do not weaken correct mathematics merely because its
proof is difficult. Existing proof placeholders may remain. Never add the conclusion as a hypothesis,
contradictory assumptions, axioms, unsafe code, or other proof loopholes.

After the complete mathematical comparison, when available, use the attached Lean MCP to request
whole-file diagnostics for every assigned Lean file and repair diagnostics in coherent batches. Do
not start another language server or invoke Lake after every edit. For the final compilation and
acceptance check, prefer the project target through `lake build`; do not use `lake env lean` when a
Lake build target is available. Run `{build_command}` once the MCP diagnostics are clean and fix all
remaining elaboration errors and all warnings except the expected warnings for deliberate `sorry`
placeholders. Edit only the assigned chapter scope and do not commit. If the complete formalization
is already faithful, well-typed, and warning-free under that exception, make no changes; a no-change
pass is the success signal used by the orchestrator. The final build must emit no warnings except the
expected warnings for deliberate `sorry` placeholders.
