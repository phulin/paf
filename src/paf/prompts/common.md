## Rules shared by all Lean stages

The assigned filesystem is not a Git repository. Do not run `git` commands or rely on Git metadata.

### Preserve the mathematics

Prefer established definitions and results from the project's Mathlib version and earlier project
chapters. Match the book's hypotheses, types, coercions, normalization choices, and source order.
Never make a theorem easier by replacing it with `True`, adding the desired conclusion as an
assumption, creating an artificial contradiction, or using axioms, unsafe declarations, `sorryAx`, or
another way around Lean's checker. A helper must state useful mathematics, appear before its users,
and not hide a circular proof. Do not add or invoke `aesop`; use focused lemmas and ordinary tactics.

### Keep imports focused and chronological

Add the focused Mathlib or stable project imports the work needs. Do not use the umbrella imports
`import Mathlib` or `import LastLib`. A section file must not import a whole book or chapter when a
specific module provides what it needs. Top-level chapter files may import section files; section
files must not import those top-level files.

In particular, a chapter may import only earlier chapters in the same book or chapters from earlier
books. Put a
genuinely shared prerequisite in `Dependencies.lean` or `Core.lean` when one exists and the result
logically belongs before every section that uses it. Do not move a later result earlier merely because
several later declarations need it.

### Leave clean, intentional changes

Fix the cause of diagnostics. Do not hide warnings with options, disable linters, or leave diagnostic-
suppression tricks in the source. Remove exploratory commands and scratch, backup, or log files before
finishing. Preserve unrelated work.

Lean accepting a file is not the same as PAF accepting it: PAF rejects every warning except the exact
warning that a declaration uses `sorry`. When a PAF handoff supplies a warning, it remains required
work even if a later tool response has no errors, reports that the file typechecks, or summarizes
warning bodies only as a count. Do not return a no-change report until every supplied warning has
either been fixed or identified as the permitted `sorry` warning.

### Keep searches and tool output bounded

Never search, list, read, or recurse into `.paf/logs`, `.paf/isolation`, or any isolation/worktree
tree. Those directories contain duplicated agent transcripts and source snapshots, not mathematical
evidence. Scope repository searches to the assigned files, earlier source modules, and Mathlib.

Keep shell and tool responses below roughly 12 KiB: use narrow paths and patterns, limit matches,
and request small source windows. If a command could emit more, add an explicit output bound or split
it into targeted queries. Lean diagnostics report target-file errors and a warning count; do not ask
for imported-file warning dumps.
