# Discover source dependencies: {book_title}, chapter {chapter_number}

## Mission

Read chapter {chapter_number}, “{chapter_title},” in `{source}` from its source span through the end
of the assigned input node. Build the direct source dependency tree needed to formalize this chapter.
This is source analysis, not target-code import discovery.

## Workflow

1. Inventory the definitions, constructions, hypotheses, and named results introduced by this input.
2. Trace each item to the earlier input nodes whose mathematical content it directly requires.
3. Compare those prerequisites with the work-unit ids in the runtime input catalog.
4. Return the minimal direct prerequisite ids in `source_dependencies`. Omit transitive ancestors and
   incidental citations. Preserve configured dependencies even when the source is terse.
5. Summarize the dependency tree and any ambiguous source references in the report.

Do not inspect generated Lean imports to infer source dependencies. Do not create or edit files, run
Lean, or attempt formalization.

## Definition of done

The complete assigned source span has been inspected and every direct cross-input prerequisite is
represented by a valid catalog id. Set `complete` to `true` only then. Set `changed` to `false`.
