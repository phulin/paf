# Repair statements after proof feedback: {book_title}, chapter {chapter_number}

## Mission

Read chapter {chapter_number}, “{chapter_title},” in `{source}`, every assigned Lean file, and the
proof/build feedback appended below. Repair only genuine statement or interface defects so the next
proof pass can proceed against faithful mathematics.

## Repair standard

Classify each obstruction as an inaccurate source translation, a missing necessary hypothesis, a
circular or divergent local API, or merely an unfinished proof. Preserve the intended mathematical
strength and make the smallest correction supported by the source and earlier theory. Update
dependent declarations inside the assigned scope consistently and reconcile provisional APIs with
canonical earlier interfaces.

Do not weaken a correct conclusion because its proof is difficult or add the result as an assumption.
Mark a substantive defect in the informal source with a precise `SOURCE_ISSUE` comment. Ordinary
formalization details—such as a necessary typeclass, finiteness, positivity, or nonzero assumption—are
interface repairs rather than source issues.

## Workflow

1. Reproduce each reported obstruction using whole-file diagnostics.
2. Separate statement defects from proof and API-lookup failures.
3. Make one coherent batch of minimal statement/interface repairs.
4. Update dependent declarations within the assigned scope without creating cyclic imports.
5. Request fresh whole-file diagnostics and repair every message caused by the changes.

## Definition of done

Every accepted repair is minimal, mathematically justified, and reported clearly; ordinary proof
difficulty remains a proof task. The assigned files have fresh diagnostics with no unexplained
messages, and the next proof agent has an exact account of every interface change.
