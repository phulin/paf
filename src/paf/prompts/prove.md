# Prove assigned declarations: {book_title}, chapter {chapter_number}

## Mission

Replace every proof hole listed in the authoritative assignment with a Lean-checked proof. The
listed declarations and holes are the exact scope; preserve other proofs and all established public
interfaces. The current Lean statements are authoritative, while the corresponding passage of
`{source}` supplies the default mathematical argument.

## Working method

1. Inspect the listed holes and their surrounding declaration. Make a checklist by proof hole or by
   tightly coupled group, followed by final validation.
2. Read only the definitions, local lemmas, source passage, and earlier APIs needed for the active
   goal. Confirm theorem signatures before using them.
3. Try checked approaches and use the residual goal to guide the next change. Prefer an exact earlier
   result, focused rewriting or simplification, a standard constructor or equivalence, and then a
   lower-level construction.
4. Extract focused private helpers when they make a large proof easier to check. Add a public helper
   only when it is genuinely reusable and naturally belongs in this chapter; request an upstream
   result instead when its proper owner is an earlier chapter.
5. Preserve independent clean progress. Remove speculative edits, unused helpers, and abandoned
   imports before finishing.
6. After the last edit, prepare affected dependencies once and request fresh diagnostics for edited
   files in import order. Resolve all errors and non-`sorry` warnings.

On a retry, use only the target-specific handoff below. Do not repeat an unchanged blocker: return
its ID in `blocker_refs`. If the handoff contains a current PAF diagnostic, repair it before
returning.

## Proof and interface constraints

- Do not change an existing public declaration's kind, name, namespace, arguments, hypotheses,
  result type, attributes, or section behavior.
- You may replace assigned proof bodies, revise private helpers used by them, add focused imports,
  and add fully proved private helpers.
- Do not add `sorry`, `admit`, axioms, unsafe declarations, `sorryAx`, artificial contradictions,
  circular helpers, diagnostic suppression, or heartbeat-limit workarounds.
- Do not add or invoke `aesop`. Use focused lemmas and ordinary tactics.
- Leave proof holes outside the authoritative assignment unchanged.

If concrete mathematics shows that a fixed statement or interface is defective, leave it unchanged
and report the smallest required correction. If a missing reusable result belongs to an earlier
chapter, report an `upstream_requests` entry after checking at least two alternatives. Continue with
independent assigned holes when possible.

## Completion and report

Set `complete` to `true` exactly when every assigned hole is replaced by a checked proof and no
assigned-span error or non-`sorry` warning remains. Otherwise retain clean progress and set it to
`false`.

Return the provided structured report only after edits and tool use stop. Keep `summary` concise.
Use `failed_attempts` only for new or materially changed target-specific evidence, `blocker_refs` for
unchanged supplied blockers, `source_issues` only for genuine textbook defects, and
`upstream_requests` only for missing results owned by earlier chapters. Leave unused lists empty.
