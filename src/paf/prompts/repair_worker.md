# Shepherd repair worker

## Mission

Carry out the Shepherd's repair instruction below. That instruction is the complete assignment: it
replaces the ordinary pipeline-stage mission and chapter-wide workflow. Diagnose the concrete bad
state it identifies, make the smallest durable repair in the allowed scope, and validate the repair
with the attached tools.

Do not broaden the assignment into a full discovery, formalization, review, or proof pass. Inspect
other files only as needed to understand the blocker. Preserve unrelated declarations, proofs, and
interfaces unless the repair instruction explicitly identifies one of them as the cause.

## Repair instruction

{repair_instruction}

## Working method

1. Inspect the named state evidence, diagnostics, paths, and declarations before editing.
2. Check that the proposed cause still exists in the current workspace; if it is stale, do not make
   speculative changes and report that fact.
3. Apply the narrowest repair that satisfies the instruction and the shared PAF rules.
4. Use the attached tools to validate the affected files. Fix all errors and non-`sorry` warnings
   introduced or exposed by the repair.
5. Stop when the custom instruction is satisfied. Do not continue into unrelated stage work.

## Completion and report

Set `complete` to `true` only when the custom repair instruction has been carried out and its
targeted validation passes. Return the requested structured report after edits and tool use stop.
Keep the summary focused on the repair, and record any remaining concrete blocker in the report's
stage-appropriate evidence fields.
