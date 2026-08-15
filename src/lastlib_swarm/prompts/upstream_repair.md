# Targeted upstream interface repair: {book_title}, chapter {chapter_number}

## Mission

Resolve the durable request batch prepended to this prompt within the assigned earlier chapter.
This is a narrow proof-support task, not a fresh chapter review and not a general placeholder pass.
For every request, determine whether the required result should be added here, already exists under a
usable declaration, or intrinsically depends on downstream-only data and therefore belongs with its
consumer.

## Repair standard

Treat each consumer residual goal and attempted-alternatives ledger as concrete evidence, not as an
instruction to add the requested theorem mechanically. Search canonical pinned Mathlib and earlier
LastLib declarations first. Check the informal excerpts and the actual types on both sides of the
proposed bridge. Prefer the weakest natural reusable result that matches the upstream chapter's
mathematical vocabulary and chronology.

If the interface is genuinely missing and belongs in this scope, add it at the earliest valid point
and prove it completely. A targeted upstream repair may not add `sorry`, `admit`, an axiom, an unused
helper, or a theorem engineered merely to restate the consumer's final goal. Preserve existing
declaration interfaces and unrelated proofs. If a sufficient declaration already exists, make no
cosmetic alias; return its exact fully qualified name and concrete application guidance. If the
bridge needs hypotheses, constructions, or vocabulary introduced only by the consumer, leave the
upstream scope unchanged and explain precisely why the proof must instead construct the bridge
downstream.

## Workflow

1. Read the entire supplied request batch before editing so requests with the same mathematical
   bridge can share one answer.
2. Inspect the supplied owner paths, consumer statements, residual goals, and relevant textbook
   excerpts. Use focused declaration lookup and searches for additional context.
3. For each request, test whether an existing declaration solves the stated interface need.
4. When an upstream addition is warranted, implement and prove the smallest reusable interface in
   the assigned scope. Reuse one declaration for multiple requests when their needs coincide.
5. Use the attached Lean MCP to inspect goals and signatures. Prepare the maximal edited dependents
   once, then obtain clean diagnostics for the edited closure.
6. Return one `upstream_answers` entry for every request id. One entry may name several request ids
   only when the disposition, exact declarations, and usage guidance are genuinely identical.

Do not edit a consumer or any other chapter. Do not work on unrelated placeholders in the owner.

## Definition of done

Every supplied request id has an exact durable answer:

- `added`: name every fully proved declaration added in this scope and explain how the consumer
  should apply it;
- `existing`: name every exact existing declaration and explain the required arguments, rewrites, or
  coercions; or
- `downstream`: leave `declarations` empty and give a precise `rejection_reason` identifying the
  consumer-only data or chronology that makes an upstream bridge inappropriate, plus downstream
  usage guidance.

Leave `upstream_requests` empty. Set `complete` to true only when the batch has an answer for every
request and every in-scope edit is clean and fully proved.
