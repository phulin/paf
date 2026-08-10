# Repair statements after proof feedback: {book_title}, chapter {chapter_number}

Read chapter {chapter_number}, “{chapter_title},” in `{source}`, the complete assigned Lean chapter,
and the proof/build feedback appended by the orchestrator.

Diagnose whether each reported obstruction is an inaccurate source translation, a missing necessary
hypothesis, a circular or divergent local API, or merely an unfinished proof. Change statements only
for genuine statement/interface defects. Preserve the intended mathematical strength and make the
smallest correction supported by the source and earlier theory. Update dependent declarations inside
this chapter consistently and reconcile guessed APIs with canonical earlier interfaces.

Do not solve proof difficulty by weakening conclusions, adding results as assumptions, inventing
contradictions, or using tautologies, axioms, unsafe declarations, or proof-checking loopholes. Mark a
genuine defect in the informal source with a precise `SOURCE_ISSUE` comment.

Keep repairs within an acyclic import graph. Add focused imports freely when repairs need them; do
not optimize for the fewest imports. Never
introduce the exact umbrella imports `import Mathlib` or `import LastLib`, or a book/chapter
aggregator into a production section when a focused module exists. Prefer a focused Mathlib module or stable
LastLib API, and do not add a section-to-section edge unless the repaired declarations genuinely use
it.

When available, use the attached Lean MCP to diagnose every assigned file before editing, then make a
coherent repair pass and iterate only over declarations whose diagnostics remain. Use goals,
declaration lookup, code actions, and fresh whole-file diagnostics. Do not run Lake, raw Lean, or
another compiler. After you finish, the coordinator merges accepted changes and serially runs
`{build_command}` in the main worktree against its single writable cache. Fix all diagnostics
available through the MCP, edit only the assigned chapter scope, remove scratch files and
exploratory commands, and do not commit. Clearly report what was repaired so the next proof pass can
proceed efficiently.
