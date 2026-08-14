# Proof-triggered statement review and repair: {book_title}, chapter {chapter_number}

## Mission

A proof attempt exposed possible defects in the statements or supporting interfaces of chapter
{chapter_number}, “{chapter_title}.” Re-review the entire assigned source scope against book
{book_title}, evaluate every proof finding supplied by the coordinator, and directly make every
warranted in-scope repair. This is a statement-review pass, not a proof or narrow fixup pass: do not
assume a reported finding is correct, and do not limit the audit to the cited declaration or file.

## Current state

Use the line-numbered source set prepended to this prompt as your initial read of `{source}` and every
assigned Lean file under `{lean_root}/{chapter_path}/` plus `{lean_root}/{chapter_path}.lean`. Review
that complete scope even when the proof findings mention only one item. Do not reread a complete
supplied file from the filesystem; inspect the filesystem only for explicitly missing or truncated
content, post-edit content, or a targeted search or lookup.

The coordinator findings record obstacles observed by a proof agent. Treat them as evidence to
investigate, not instructions to apply mechanically. For each finding, decide whether the problem is
an inaccurate or overly strong statement, a missing hypothesis, a poor local interface, a missing
earlier bridge, an invalid proof strategy, or no defect at all. Repair statement and API defects in
scope. If the statement is sound and the reported obstruction is only a failed proof approach,
preserve it and explain that conclusion in the final summary.

## Review standard

Compare source and Lean declaration by declaration. Check complete source coverage, quantifier scope,
implication direction, hypotheses, domains and codomains, coercions, indexing, normalization,
mathematical strength, dependency order, and a plausible proof route through earlier LastLib and
pinned Mathlib APIs. Repair inaccurate, circular, vacuous, or unprovable interfaces in place. Existing
proof placeholders may remain; use `by sorry` for new propositions rather than proving them here.

For each principal result, trace the informal argument far enough to distinguish a genuine statement
or interface defect from a proof-search failure. Add a support declaration only when it provides a
genuinely absent or materially more usable interface. Use the weakest natural assumptions, place it
before its users, and keep every import chronological.

## Workflow

1. Build the assigned files' local import order from the supplied content and audit the entire scope
   from prerequisites to dependents.
2. Audit every declaration for source fidelity, mathematical provability, proof readiness, and
   chronological dependencies.
3. Evaluate every coordinator finding explicitly and make the minimal warranted in-scope repairs.
4. Continue the full-scope audit after resolving the supplied findings; they are starting evidence,
   not the boundary of this review.
5. For a required repair owned by another chapter, emit a precise `fixup_findings` entry naming its
   exact repository-relative Lean paths. Do not delegate an in-scope repair that you can make here.
6. Use the attached Lean MCP on demand for APIs and changed-file diagnostics. After the last edit,
   prepare the maximal changed dependents once and check the edited closure in import order.
7. Return a structured report that says which proof findings were confirmed, rejected, or reframed,
   what was repaired, and what remains.

Do not run Lean, Lake, or another language server; use only the attached Lean MCP. Edit only the
assigned scope. A no-change review needs no diagnostic calls because the incoming coordinator build
is authoritative. After changes, resolve every diagnostic except the exact “declaration uses `sorry`”
warning before finishing.

## Definition of done

The complete assigned scope has been re-reviewed, every supplied proof finding has been evaluated,
all warranted in-scope statement and interface repairs have been made, imports remain chronological,
and the chapter is ready for coordinator rebuild and a fresh proof attempt. Remaining source-changing
work is reported only when its true owner lies outside this scope.
