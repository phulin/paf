# Proof blocker review and upstream capability routing

## Status

Implementation design. This document replaces the failed-proof review loop's implicit rule that an
unfilled placeholder should be reopened after every review. The implementation is intentionally
incremental and preserves existing persisted state.

## Problem

The proof pipeline records useful failure evidence, but its two recovery mechanisms lose the
meaning of that evidence:

1. Proof review returns `confirmed`, `rejected`, or `reframed`, while the scheduler reopens every
   reviewed blocker whose placeholder remains. The assessment does not select a next action.
2. Proof review cannot create an upstream request, so it cannot correct a proof agent's ownership
   or routing mistake.
3. Upstream requests are identified by the consumer rather than by the missing mathematical
   capability. Equivalent requests from different consumers are not shared.
4. An owner batch is all-or-nothing, and an answer is considered useful before it has passed a
   consumer-facing acceptance check.
5. Unchanged evidence, zero-work session resumptions, stale targets, and unrelated dependency
   failures can consume semantic retry budgets.

The result is repeated review prose, premature escalation, and proof retries with no new premise.

## Invariant

PAF retries a blocked proof only when it can persist a **retry cause** whose digest differs from the
blocker's last attempted retry cause. A retry cause is one of:

- a source or interface edit;
- a validated upstream capability answer;
- a review-supplied executable proof route;
- repaired coordinator diagnostics;
- an explicit operator action.

Review completion alone is not a retry cause.

## 1. Proof review is a routing decision

### Report contract

Replace the ambiguous assessment-only contract with one resolution per finding:

```json
{
  "finding_id": "request:1",
  "finding": "concise finding identity",
  "diagnosis": "missing_capability",
  "action": "request_upstream",
  "explanation": "checked evidence",
  "retry_contract": null,
  "upstream_requests": []
}
```

Supported diagnoses:

- `statement_defect`
- `interface_defect`
- `missing_capability`
- `consumer_local_proof`
- `stale_target`
- `external_gap`
- `validation_noise`
- `genuine_blocker`

Supported actions:

- `repair_and_retry`
- `retry_with_route`
- `request_upstream`
- `send_to_roadmap`
- `wait_for_dependency`
- `park_external`
- `drop_stale_target`

The legacy `assessment` field remains accepted while persisted and older test fixtures migrate, but
new reports use `diagnosis` and `action`.

### Scheduler semantics

- If the declaration no longer has a placeholder, resolve the blocker.
- `repair_and_retry` requires a changed source digest.
- `retry_with_route` requires a nonempty executable retry contract and records its digest.
- `request_upstream` validates and enqueues the supplied capability requests, then marks the blocker
  `upstream_requested`.
- `send_to_roadmap`, `park_external`, and an unresolved `genuine_blocker` mark the blocker `blocked`.
- `wait_for_dependency` leaves the blocker waiting without incrementing semantic review exchanges.
- `drop_stale_target` resolves the blocker only after the declaration lookup confirms that the
  target no longer exists or no longer contains a placeholder.

No other outcome reopens a blocker.

### Prompt

The review prompt is a bounded triage prompt. It must:

1. confirm that the target is live and the evidence digest is current;
2. distinguish statement defects, missing capabilities, local proof work, stale targets, and
   validation noise;
3. call a route executable only when every substantial step names an exact existing declaration and
   a focused Lean probe checks the critical composition;
4. emit an upstream request whenever a required substantial step is absent;
5. avoid repeating searches in the blocker ledger;
6. avoid a full dependency build when it made no source edit.

## 2. Executable retry contracts

A proof retry contract contains:

- `new_information`: what changed relative to the failed attempt;
- `declarations`: exact fully qualified declarations;
- `intermediate_claims`: the dependency-ordered mathematical steps;
- `critical_probe`: the focused Lean probe that checked the nontrivial composition;
- `known_remaining_gap`: empty for an executable retry.

The scheduler hashes the normalized contract. It permits one proof retry for a new contract digest
and will not send the same contract through another review/proof cycle without changed source.

## 3. Capability-centric upstream requests

### Identity

An upstream request has a stable `capability_key`. When omitted by an older agent, PAF derives one
from the normalized desired declaration/type (`needed_result`) and owner placement. The canonical
fingerprint excludes consumer identity.

One request record stores a `consumers` array. Each consumer attachment contains:

- consumer chapter, path, and blocked declaration;
- residual goal;
- origin runs and attempted alternatives;
- an acceptance contract.

Equivalent requests attach to the same capability record. Existing single-consumer fields remain
materialized for checkpoint compatibility.

### Ownership

Ownership has a `kind`:

- `chapter`: a selected chronological chapter;
- `consumer`: the result naturally belongs beside the blocked proof;
- `shared`: a shared support module selected by an explicit path;
- `external`: Mathlib or another unavailable dependency.

The first implementation continues to execute only `chapter` owners automatically. Other kinds are
persisted as routed/parked outcomes rather than being rejected as malformed. This separates safe
routing from automatic write authority.

### Request content

Each request includes:

- `capability_key`;
- desired result and optional candidate declaration signature;
- placement kind and owner paths;
- attempted alternatives;
- `acceptance_tests`, each naming a consumer declaration and the expected first nontrivial
  application/reduction.

The router, not the proof agent, resolves exact path ownership and chronological safety.

## 4. Partial upstream answers

Upstream answers are independent per capability. Supported dispositions are:

- `added`
- `existing`
- `partial`
- `downstream`
- `external`
- `decompose`

An owner run need not answer every request successfully. Valid answers are persisted individually;
missing or invalid answers remain requested with an attempt error. They do not force unrelated
answers into escalation.

`partial` records fully proved declarations that reduce the capability but do not satisfy its
acceptance contract. `decompose` creates smaller child capability requests. `downstream` routes to a
consumer roadmap unless it includes an executable retry contract. `external` parks the request.

## 5. Acceptance before downstream retry

Owner validation proves only that an interface exists and is clean. A capability becomes
`consumer_validated` only when:

1. every declared support result resolves without a placeholder;
2. the owner build is clean;
3. the answer supplies concrete usage guidance;
4. a consumer acceptance probe or a targeted proof retry reaches the named blocked declaration.

The first implementation represents acceptance tests and validates structural preconditions before
retry. The downstream retry is the semantic acceptance probe. Success closes only the capability
attachments whose blocked declarations lost their placeholders; other attachments remain active.

## 6. Evidence-gated wake-up

Blockers persist:

- consumer source digest at observation;
- owner/API digest when answered;
- normalized residual-goal digest;
- last retry-cause digest;
- last review-resolution digest.

PAF wakes a consumer only when one of these relevant inputs changes. Repeated sightings update
telemetry but do not create work.

## 7. Session and dependency failures

- A report-correction resume with zero invocation tokens and no new valid structured resolution is
  an infrastructure failure, not a semantic exchange.
- PAF permits one same-session schema correction. A second correction starts a fresh session with
  bounded accumulated evidence. Further failure is recorded as `review_infrastructure_failed`.
- Unrelated prerequisite build failures produce a `wait_for_dependency` resolution. They do not
  increment proof-review exchange counts or mark the proof mathematically failed.

## 8. Roadmap routing

Sound but substantial consumer-local work is sent to the existing Shepherd repair system. The
review resolution supplies the exact declaration, the blocker ledger, and the required intermediate
lemmas. Shepherd writes the source-adjacent roadmap; the normal proof stage wakes only after that
scope digest changes.

This avoids inventing a second roadmap subsystem.

## 9. Observability

Persist counters/events for:

- review resolutions by diagnosis/action;
- no-op reviews;
- zero-work resumptions;
- deduplicated capability attachments;
- owner answers by disposition;
- consumer acceptance successes/failures;
- retries suppressed because the evidence digest was unchanged;
- stale targets and dependency waits.

Task detail should describe the routing state (`waiting for capability X`, `roadmap required`,
`waiting for dependency Y`) rather than a generic retry-cap failure.

## 10. Compatibility and rollout

The state database stores these objects as JSON state items, so the rollout does not require a SQL
schema migration. Normalization supplies defaults for old records:

- legacy proof assessments map to conservative actions (`rejected` to `retry_with_route` only when
  an executable contract is present; otherwise `send_to_roadmap`);
- legacy upstream records gain one consumer attachment and a derived capability key;
- legacy `answered` requests remain eligible for their existing single targeted retry;
- old status values remain readable.

Implementation order:

1. report schemas, prompt, normalization, and routing semantics;
2. capability identity and multi-consumer state;
3. partial answers and per-consumer closure;
4. evidence gating, zero-work resume handling, and dependency waits;
5. regression tests modeled on the observed graded Hilbert polynomial, Hilbert--Samuel dimension,
   inverse-limit grading, smooth fibre dimension, rejected colimit target, and Serre criterion
   cases.
