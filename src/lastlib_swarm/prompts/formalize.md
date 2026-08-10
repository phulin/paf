# Formalize statements: {book_title}, chapter {chapter_number}

You own the complete statement-formalization pass for chapter {chapter_number},
“{chapter_title},” of `{source}`. Work from the repository root and read the full source chapter from
its numbered heading through the next heading of the same level.

Create or update the Lean chapter at `{lean_root}/{chapter_path}/` and its aggregator
`{lean_root}/{chapter_path}.lean`. Preserve the source order and represent every substantive,
mathematically precise assertion: labeled declarations, displayed identities and diagrams, exact
sequences, compatibility results, hypotheses embedded in prose, examples, and precise warnings.
Skip motivation, proof sketches, history, and redundant paraphrases.

This is a statement pass, not a proof pass. Proofs may use `by sorry`; definitions should have real
bodies whenever the canonical construction is clear. Every declaration type must expose the exact
informal mathematics so later proof agents do not have to redesign the API.

Before inventing an interface, search pinned Mathlib and already-established project modules. Reuse
canonical definitions. Match all finiteness, separation, completeness, characteristic, normalization,
and typeclass assumptions precisely. Never use `True`, contradictory hypotheses, axioms, unsafe code,
or tautological definitions as stand-ins. Mark a false or underspecified source assertion with a
`SOURCE_ISSUE` comment and make only the minimal principled correction.

Other chapters may be processed concurrently. Do not inspect or edit another in-progress chapter.
If an unavailable earlier result is genuinely required, use a clearly marked, mathematically natural
dependency guess local to this chapter; do not engineer it to imply the desired conclusion.

Cover the entire source chapter before polishing optional bodies. For compilation and testing, prefer
the project target through `lake build`; do not use `lake env lean` when a Lake build target is
available. Run `{build_command}`, fix every elaboration error and every warning except the expected
warnings for deliberate `sorry` placeholders, remove scratch files and exploratory commands, and
edit only the assigned chapter scope. The final build must emit no warnings except the expected
warnings for deliberate `sorry` placeholders. Do not commit.
