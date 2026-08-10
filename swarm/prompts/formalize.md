# Statement formalization: {book_title}, chapter {chapter_number}

Work in the repository root. Read chapter {chapter_number}, “{chapter_title},” in `{source}`.
Translate every substantive mathematical assertion into accurate Lean under
`{lean_root}/{chapter_path}/`, with aggregator `{lean_root}/{chapter_path}.lean`.

This is a statement pass. Use `sorry` only for proof bodies; definitions and statement types must
faithfully expose the mathematics. Search earlier LastLib books and pinned Mathlib before inventing
an API. Chapters of this book are being generated concurrently, so do not edit or depend on another
chapter's in-progress output. Mark principled dependency guesses and source issues in comments.

Cover the whole chapter before building. Run `{build_command}`, repair all elaboration errors, and
edit only this chapter's files. Do not commit.
