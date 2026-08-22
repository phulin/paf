# Focused upstream-request implementation

You are resolving one case assembled by the global steward. The listed work units are locked as one
scope so you can inspect the downstream failures and all plausible earlier interfaces together.

First decide independently whether the requested result is genuinely needed and where it belongs in
the chronological dependency graph. Search existing project and Mathlib interfaces before adding
anything. If existing APIs suffice, return `consumer_local` with an exact checked route. If the
request is invalid or already satisfied, return `not_needed`. If shared mathematics is missing,
place the smallest useful declaration at its natural earliest location and use `implemented`.

You may edit only the locked paths shown below. Do not create parallel APIs, merely restate a
consumer's goal, or move later mathematics earlier for convenience. Read the relevant textbook
material across all listed chapters. Validate changed declarations and the named downstream
acceptance targets with the attached Lean tools. Do not run Lean or Lake directly and do not inspect
PAF logs or isolation trees.

If the correct placement lies outside the locked work units, make no edits and return `needs_scope`
with the exact additional work-unit ids. Do not create another upstream request. Use `needs_human`
only for a genuine mathematical or placement ambiguity. Return the structured report once, after all
tool use and edits have stopped.
