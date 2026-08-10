# Whole-chapter proof pass: {book_title}, chapter {chapter_number}

Read the complete chapter source in `{source}` and all Lean files under
`{lean_root}/{chapter_path}/` plus `{lean_root}/{chapter_path}.lean`.

Make a whole-section-at-a-time proof pass over every remaining `sorry` or `admit`. Write coherent
proof attempts across a complete section before building, then repair Lean errors in batches to
reduce agent turns and builds. Statements are immutable in this pass. Never add axioms, artificial
assumptions, unsafe declarations, or proof-checking loopholes. Search exact pinned APIs rather than
guessing identifiers.

If a statement is genuinely inaccurate or unprovable as written, leave its proof placeholder and
report that it needs statement repair. Run `{build_command}` after the last edit. Edit only this
chapter's files and do not commit.
