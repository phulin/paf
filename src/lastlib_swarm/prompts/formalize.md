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

Source coverage alone is not sufficient. Read the informal proofs and proof sketches and outline a
plausible proof dependency route for every principal result. Include every genuine intermediate
lemma that route needs and that is not already available from earlier project modules or pinned
Mathlib, even when the source calls it obvious, uses it only inside a proof, or silently switches
between equivalent formulations. In particular, look for basic `↔` lemmas and equivalences between
book-facing and canonical Lean formulations; constructor/eliminator and extensionality facts;
membership, coercion, map, restriction, and normalization lemmas; closure and functoriality facts;
and short bridges between adjacent proof steps.

Search for canonical declarations before adding proof support. When the existing interface is
adequate, use it directly; otherwise add a meaningful chapter-facing bridge with the weakest natural
assumptions and place it before its first user. Such lemmas may contain `by sorry` in this pass, but
must themselves be accurate and provable from earlier material. Do not introduce a helper that
merely restates its target, assumes the target's conclusion, depends on a later declaration, or hides
circularity. Keep elaboration-only scaffolding private, but expose reusable equivalences and bridges
that downstream chapter proofs need.

Before inventing an interface, search pinned Mathlib and already-established project modules. Reuse
canonical definitions. Match all finiteness, separation, completeness, characteristic, normalization,
and typeclass assumptions precisely. Never use `True`, contradictory hypotheses, axioms, unsafe code,
or tautological definitions as stand-ins. Mark a false or underspecified source assertion with a
`SOURCE_ISSUE` comment and make only the minimal principled correction.

Other chapters may be processed concurrently. Do not inspect or edit another in-progress chapter.
If an unavailable earlier result is genuinely required, use a clearly marked, mathematically natural
dependency guess local to this chapter; do not engineer it to imply the desired conclusion.

Maintain an acyclic import graph. Add as many focused Mathlib or stable LastLib imports as the file
needs; do not optimize for the fewest imports. Never use the exact umbrella imports `import Mathlib` or `import LastLib`, and
never import a whole book or chapter aggregator from a section when a focused module exists. Do not mirror prose order with a linear
section-to-section import chain unless declarations are genuinely used downstream. Put shared
interfaces in the chapter's `Dependencies.lean` or `Core.lean` when those files exist; aggregators
may import sections, but sections must not import aggregators. Preserve this policy when creating
new files.

Cover the entire source chapter before beginning the check-and-repair loop. When available, use the
attached Lean MCP to request whole-file diagnostics for every assigned Lean file, fix diagnostics in
coherent batches, and request fresh diagnostics after each batch. Do not start another language
server or invoke Lake after every edit. For the final compilation and acceptance check, prefer the
project target through `lake build`; do not use `lake env lean` when a Lake build target is available.
Run `{build_command}` once the MCP diagnostics are clean, fix every remaining elaboration error and
every warning except
the expected warnings for deliberate `sorry` placeholders, remove scratch files and exploratory
commands, and edit only the assigned chapter scope. The final build must emit no warnings except the
expected warnings for deliberate `sorry` placeholders. Do not commit.
