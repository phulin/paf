# Whole-chapter proof pass: {book_title}, chapter {chapter_number}

Read the full informal chapter in `{source}` and all assigned Lean files under
`{lean_root}/{chapter_path}/` plus `{lean_root}/{chapter_path}.lean`. Inventory every remaining
`sorry` and `admit` before editing.

Statements and imports are immutable during this pass. Prove as many placeholders as possible using
only earlier declarations and pinned project/Mathlib APIs. Never change binders, hypotheses, result
types, declaration kinds, namespaces, attributes, imports, or section behavior to make a proof easier.
Never add axioms, unsafe declarations, artificial contradictions, or kernel-checking loopholes.

Work section-by-section: write a coherent proof attempt for a whole section before the first build,
then run `{build_command}` and repair errors in batches. Confirm exact theorem signatures from source
rather than guessing names. One hard declaration must not prevent independent later proofs.

If a target is mathematically inaccurate or cannot follow from its stated assumptions, leave its
placeholder, avoid changing the statement, and report a precise statement/API repair request. After
the final edit, run a fresh successful build. Edit only the assigned chapter scope, remove scratch
files and exploratory commands, and do not commit.
