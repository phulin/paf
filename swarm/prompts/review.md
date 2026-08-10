# Statement review: {book_title}, chapter {chapter_number}

Compare chapter {chapter_number}, “{chapter_title},” in `{source}` with all Lean files scoped by
`{lean_root}/{chapter_path}.lean` and `{lean_root}/{chapter_path}/`.

Review every statement for faithful quantifiers, hypotheses, normalization, mathematical strength,
source order, and use of canonical earlier APIs. Add missing substantive claims and make the smallest
principled correction to inaccurate or unprovable statements. Reconcile dependency guesses with
earlier books and chapters where possible. Existing proof `sorry`s may remain.

Run `{build_command}` and edit only this chapter's files. If the chapter is already completely
accurate, make no changes. Do not commit.
