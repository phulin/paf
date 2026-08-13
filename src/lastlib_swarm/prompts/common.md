## Common Lean policy

The assigned filesystem is not a Git repository. Do not run `git` commands or rely on Git metadata.

### Mathematical integrity

Use canonical pinned Mathlib and established LastLib APIs before introducing new interfaces. Match
the intended hypotheses, types, coercions, normalization conventions, and source order precisely.
Never use `True`, vacuous implications, artificial contradictions, the desired conclusion as a
hypothesis, axioms, unsafe declarations, `sorryAx`, or another kernel-checking loophole. A helper
must express reusable mathematics, precede its users, and not conceal a circular proof.
Do not add or invoke `aesop`; use explicit lemmas and focused ordinary tactics.

### Imports

Add as many focused Mathlib or stable LastLib imports as the work requires; they do not need to be
minimized. Never add the exact umbrella imports `import Mathlib` or `import LastLib`. A production
section must not import a book or chapter aggregator when a focused module provides the API, and it
must not import another section merely to mirror prose order. Aggregators may import leaves; leaves
must not import aggregators. Preserve chronological dependencies: a chapter may import only earlier
chapters in the same book and chapters from earlier books, never a later chapter or later book. Put
genuinely shared chapter interfaces in `Dependencies.lean` or `Core.lean` when either exists.

### Diagnostics and deliverables

Resolve diagnostics by fixing their cause. Do not hide a warning with `set_option`, disable a linter,
or leave diagnostic-suppression tricks in the source. Remove exploratory commands and all scratch,
backup, or log files before finishing. Preserve unrelated work.
