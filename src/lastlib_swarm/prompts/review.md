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

Perform the review in this order:

1. Audit source coverage and the mathematical fidelity of every declaration.
2. Audit proof readiness, dependency routes, and missing reusable interfaces.
3. Repair inaccurate, circular, vacuous, or unprovable statements minimally.
4. Obtain clean whole-file diagnostics for the assigned scope.
5. Audit imports in every production Lean file changed or reviewed. Focused imports may be added
   freely and do not need to be minimized. Replace the exact umbrella imports `import Mathlib` and
   `import LastLib`, book/chapter aggregators in leaf modules, and prose-order section chains that are
   not genuine declaration dependencies. Prefer focused Mathlib modules and stable LastLib
   `Dependencies.lean`, `Core.lean`, API, or precise section modules. Aggregators may import leaves;
   leaves must not import aggregators.
6. Finish the source review; the coordinator will run the post-merge targeted build.

After the complete mathematical comparison, when available, use the attached Lean MCP to request
whole-file diagnostics for every assigned Lean file and repair diagnostics in coherent batches. Do
not start another language server or run Lake, raw Lean, or another compiler. After you finish, the
coordinator merges accepted changes and serially runs `{build_command}` in the main worktree against
its single writable cache. Edit only the assigned chapter scope and do not commit. If the complete
formalization is already faithful and well-typed under MCP diagnostics, make no changes; a no-change
pass is the success signal used by the orchestrator.
