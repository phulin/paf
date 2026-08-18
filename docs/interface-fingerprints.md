# Lean interface fingerprints

PAF separates source freshness, compiled artifacts, and downstream-visible Lean interfaces.
Successful coordinator builds record, per owned module:

- the Lake `.olean.hash` artifact digest;
- a SHA-256 digest of proof-erased `Lean.ModuleData`;
- direct imports read from the compiled module;
- the exact Lean version and fingerprint schema.

The work-unit interface digest is an order-independent aggregate of all modules in its target scope.
The collector reuses a module record when its artifact digest, schema, and Lean version are unchanged,
so a normal incremental build fingerprints only the edited modules.

The proof/typechecking fingerprint erases theorem and opaque values but retains ordinary definition
bodies, declaration types, reducibility data, inductive metadata, and elaboration-relevant persistent
environment extensions. Source ranges, module-use telemetry, and compiler IR extensions are excluded.
Unreferenced private declarations are excluded; private declarations reachable from exported types or
definition bodies remain part of the fingerprint.

## Invalidation modes

Set `swarm.interface_invalidation` to one of:

- `observe`: collect fingerprints but retain the legacy source-graph invalidation policy.
- `conservative` (default): skip downstream invalidation only when both old and new interface digests
  are known and equal. Changed, missing, or failed fingerprints use the legacy closure.
- `interface`: use the compiled import graph wherever records exist, with the discovered source graph
  as a migration fallback for work units not yet fingerprinted.

An edit initially makes only its own source/artifact record stale. Its previously compiled interface
remains current until the rebuild finishes. If the rebuilt interface differs, PAF marks only the
compiled-import successor closure `interface_stale` and queues those units for automatic rebuild.
Their local proof-completion history remains intact.

State exposes `proof_complete`, `interface_current`, `dependencies_current`, `head_build_status`, and
`fully_certified` projections for each task. The dashboard reports locally completed proofs separately
from proofs fully certified against HEAD.

`formalize_graph.fingerprint_metrics` records interface-preserving edits, interface-changing edits,
queued descendants, automatic successful rechecks, fingerprint failures, and conservative fallbacks.
