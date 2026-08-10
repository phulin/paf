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

For compilation and testing, prefer the project target through `lake build`; do not use
`lake env lean` when a Lake build target is available. Run `{build_command}`, fix all elaboration
errors and all warnings except the expected warnings for deliberate `sorry` placeholders, edit only
the assigned chapter scope, remove scratch files and exploratory commands, and do not commit. Clearly
report what was repaired so the next proof pass can proceed efficiently. The final build must emit
no warnings except the expected warnings for deliberate `sorry` placeholders.
