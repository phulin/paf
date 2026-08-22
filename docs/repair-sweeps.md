# Upstream request architecture

PAF uses an upstream request when a downstream proof exposes a problem that cannot be assigned by
looking at either side alone. Examples include a missing predicate result, an upstream theorem that
is too weak, a misleading definition or instance, and an API that is sound but poorly exposed.

An upstream request is an observation, not a work package. It records:

- the blocked downstream declaration, residual goal, and checked attempts;
- the result the consumer appears to need;
- suspected earlier paths, without granting ownership of them;
- the evaluator's decision and the run that validated it; and
- the original blocker and source digest needed to retry the consumer safely.

Requests have five durable states: `open`, `evaluating`, `verified`, `rejected`, and `failed`.
They have no lease, plan, worker tree, path reservation, or package dependency graph.

## Incident evaluation

The deterministic escalation detector groups related live observations into one incident. A cheap
`owner_placement` scout receives the downstream obstruction, suspected earlier
paths, compact prior trace evidence, and configured work-unit map. It inspects both sides and
recommends one of these outcomes:

1. Repair a false, missing, or too-weak upstream interface and validate the owner scope.
2. Reject upstream placement with a checked, executable route using the existing interface.
3. Wait for a real chronological dependency.
4. Park a genuinely external dependency or request rare arbitration.

A validated upstream edit marks the request `verified`; checked rejection guidance marks it
`rejected`. Either result reopens the consumer proof, which is the final semantic acceptance test.
A repair-agent or validation failure may use the incident's single read-only arbitration. Unchanged
evidence is parked rather than relaunched. Repair agents run as auxiliary review work using the
cheap worker profile: they own interface and structural changes, while proposition proofs introduced or
invalidated by those edits are returned to ordinary proof agents. See
[incident-scoped escalation](escalation-coordination.md).

## Scheduling and deduplication

The proof-blocker ledger remains the source of identity. Repeated sightings of the same blocker
attach to the same request, and deterministic signal/evidence digests suppress identical work.
Related requests are grouped by capability instead of being reclassified with the full
global ledger. Focused repairs obey normal multi-work-unit path isolation and serialization.

Local proof problems stay local. External gaps are parked. Only a candidate whose paths resolve to
an earlier selected work unit becomes an upstream request automatically.

## Migration from capability packages

Schema v11 converts every open package consumer into one upstream request. Package tables remain
read-only historical evidence, while Steward leases and package path reservations are removed in
the same database transaction. Package plans, steps, splits, dependencies, and worker history do
not become new work. Package execution is disabled even when an older configuration contains
`[steward] enabled = true`.

This mapping intentionally follows consumers rather than package roots: a package with no open
consumer creates no request, and a package with several blocked consumers creates one independently
verifiable request per consumer.
