# Discover source dependencies: {book_title}, chapter {chapter_number}

## Goal

Determine which earlier chapters this chapter directly needs. Read chapter {chapter_number},
“{chapter_title},” in `{source}`, restricted exactly to lines {source_start_line}-{source_end_line}
(inclusive). Do not read past line {source_end_line} when deciding dependencies. Return the ids of
its direct prerequisites so PAF can process the chapters in the right order.

This stage only studies the book source. It does not write Lean code, inspect generated Lean imports,
or edit any files.

## Workflow

1. Read the complete assigned source span, `{source}:{source_start_line}-{source_end_line}`, and no
   later chapter text.
2. List the definitions, constructions, assumptions, and named results introduced in this chapter.
3. For each item, identify the earlier chapters whose mathematical content it directly uses.
4. Match those chapters to ids in the input catalog supplied below.
5. Return those ids in `source_dependencies`, and briefly explain the dependency relationships and
   any ambiguous references in the report.

## Guardrails

A direct prerequisite is one this chapter actually uses, not merely one that an earlier prerequisite
uses. Do not include those indirect ancestors or incidental citations. Keep dependencies explicitly
provided by the project even when the book leaves them implicit.

Never create a dependency edge from this chapter to itself. The assigned chapter's own id must not
appear in `source_dependencies`.

Do not infer dependencies from chapter numbers or from Lean `import` lines. Do not create or edit
files, run Lean, or attempt any formalization.

## Definition of done

The full source span has been inspected, and every direct prerequisite is represented by a valid
catalog id. Set `complete` to `true` only then.

## Output format

Return the structured report once, after the analysis is finished. Use only these fields:

- `complete`: `true` only when the definition of done is met.
- `summary`: a short explanation of the direct dependency relationships.
- `issues`: unresolved ambiguities or source-access problems; otherwise an empty list.
- `source_dependencies`: the direct prerequisite chapter ids from the supplied catalog. Do not
  include this chapter, indirect prerequisites, Lean imports, or guesses based on chapter numbering.
