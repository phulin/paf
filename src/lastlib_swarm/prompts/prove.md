# Whole-chapter proof pass: {book_title}, chapter {chapter_number}

Read the full informal chapter in `{source}` and all assigned Lean files under
`{lean_root}/{chapter_path}/` plus `{lean_root}/{chapter_path}.lean`. Inventory every remaining
`sorry` and `admit` before editing.

Statements are immutable during this pass. Prove as many placeholders as possible using only earlier
declarations and pinned project/Mathlib APIs. Never change binders, hypotheses, result types,
declaration kinds, namespaces, attributes, or section behavior to make a proof easier. Adding a
focused Mathlib or LastLib import required by a valid proof is allowed.
Never add axioms, unsafe declarations, artificial contradictions, or kernel-checking loopholes.

Add as many focused imports as a proof needs; they do not need to be minimized. The forbidden
additions are the exact umbrella imports `import Mathlib` and `import LastLib`, plus book/chapter
aggregators where a focused module exists.

First make one coherent proof-writing pass over the entire assigned file set. Attempt every
mathematically sound placeholder once; do not stop to compile or diagnose each proof separately.
After that whole-file pass, when available, use the attached Lean MCP to request diagnostics for
every assigned file. Iterate only over proofs and dependent declarations that fail, using proof
goals, batched tactic attempts, code actions, declaration lookup, and fresh whole-file diagnostics.
Without the MCP, check coherent whole-file repair batches with the configured Lake target rather than
building after each proof. Confirm exact theorem signatures from source rather than guessing names.
One hard declaration must not prevent independent later proofs.

If a target is mathematically inaccurate or cannot follow from its stated assumptions, leave its
placeholder, avoid changing the statement, and report a precise statement/API repair request. After
the final edit, prefer the project target through `lake build`; do not use `lake env lean` when a Lake
build target is available. Run `{build_command}` as the final acceptance check.
It must emit no warnings except those expected from deliberate remaining `sorry` placeholders. Edit
only the assigned chapter scope, remove scratch files and exploratory commands, and do not commit.
