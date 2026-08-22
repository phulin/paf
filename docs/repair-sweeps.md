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

## Tandem evaluation

PAF assigns a focused proof-review turn to the nearest suspected earlier work unit. The prompt
contains both the downstream obstruction and the suspected upstream paths. The evaluator inspects
both sides and chooses one of these outcomes:

1. Repair a false, missing, or too-weak upstream interface and validate the owner scope.
2. Reject upstream placement with a checked, executable route using the existing interface.
3. Wait for a real chronological dependency.
4. Record a concrete implementation failure.

A validated upstream edit marks the request `verified`; checked rejection guidance marks it
`rejected`. Either result reopens the consumer proof, which is the final semantic acceptance test.
A repair-agent or orchestration error becomes `failed`; uncertainty alone is not a terminal outcome
and must be resolved through a conservatively scoped repair case. Repair agents run as auxiliary
review work: they own interface and structural changes, while proposition proofs introduced or
invalidated by those edits are returned to ordinary proof agents.

## Scheduling and deduplication

The proof-blocker ledger remains the source of identity. Repeated sightings of the same blocker
attach to the same request, so they do not create repeated review agents. Requests for one owner are
served by the ordinary focused-review queue and obey normal per-chapter agent serialization.

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
