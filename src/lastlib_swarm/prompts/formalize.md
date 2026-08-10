# Formalize statements: {book_title}, chapter {chapter_number}

## Mission

Read chapter {chapter_number}, “{chapter_title},” in `{source}` from its numbered heading through the
next heading of the same level. Create or update `{lean_root}/{chapter_path}/` and its aggregator
`{lean_root}/{chapter_path}.lean` so the chapter exposes an accurate, proof-ready Lean API in source
order.

This is a statement pass. Proofs may use `by sorry`; definitions should have genuine bodies whenever
the canonical construction is clear. Do not translate motivation, history, proof narration, or
redundant paraphrases into declarations.

## Coverage and proof readiness

Represent every precise assertion: labeled declarations, displayed identities and diagrams, exact
sequences, compatibility results, hypotheses embedded in prose, examples, and mathematically precise
warnings. Read informal proofs for dependency planning even though their narration is not itself
formalized.

For every principal result, trace a plausible route through earlier project declarations and pinned
Mathlib. Add genuinely missing intermediate interfaces, especially basic `↔` lemmas; equivalences
between book-facing and canonical formulations; constructor, eliminator, and extensionality facts;
membership, coercion, map, restriction, and normalization lemmas; and closure or functoriality
bridges. Search for a canonical declaration first. A new bridge must use the weakest natural
assumptions, precede its users, and be independently provable from earlier material.

Match finiteness, separation, completeness, characteristic, normalization, and typeclass assumptions
exactly. Mark a genuine false or underspecified source assertion with a precise `SOURCE_ISSUE` comment
and make only the minimal principled correction. If an unavailable earlier result is essential, add
only a clearly marked, mathematically natural local dependency guess—not one engineered to imply the
desired conclusion.

## Workflow

1. Inventory the complete source chapter and the existing assigned Lean files.
2. Inspect canonical earlier LastLib and pinned Mathlib interfaces.
3. Complete the statement and definition pass across the entire chapter.
4. Audit proof readiness and add only genuinely missing reusable bridges.
5. Request whole-file diagnostics for every assigned file and repair them in coherent batches.

## Definition of done

Every substantive source assertion is represented or explicitly accounted for, every declaration is
accurately typed and ordered, definitions have real bodies where practical, and fresh whole-file
diagnostics contain no unexpected messages. Report coverage gaps, dependency guesses, source issues,
and important interface choices.
