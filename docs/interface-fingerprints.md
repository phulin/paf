# Lean interface fingerprints

PAF separates source freshness, compiled artifacts, and downstream-visible Lean interfaces.
Successful coordinator builds use a `(source file, interface digest)` pair as the invalidation key.
The collector also retains artifact and import metadata to avoid recomputing fingerprints and to
recover the compiled import graph, but those fields do not decide whether an edit invalidates
downstream work. For each owned module it records:

- the Lake `.olean.hash` artifact digest;
- a SHA-256 digest of proof-erased `Lean.ModuleData`;
- direct imports read from the compiled module;
- the exact Lean version and fingerprint schema.

The collector reuses a module record when its artifact digest, schema, and Lean version are unchanged,
so a normal incremental build fingerprints only the edited modules. Downstream invalidation occurs
only when at least one file's old and new interface digests differ. Aggregate work-unit digests are
informational and do not drive invalidation.

The proof/typechecking fingerprint erases theorem and opaque values but retains ordinary definition
bodies, declaration types, reducibility data, inductive metadata, and elaboration-relevant persistent
environment extensions. Source ranges, module-use telemetry, and compiler IR extensions are excluded.
Unreferenced private declarations are excluded; private declarations reachable from exported types or
definition bodies remain part of the fingerprint.

## Invalidation policy

PAF has one baseline-first invalidation policy. The first successful fingerprint observed for a file
becomes its golden baseline and does not invalidate downstream work. Later changes to or deletion of
that observed interface mark the relevant compiled-import successor closure `interface_stale`.
Fingerprint failures leave the owner stale without manufacturing a downstream change. The compiled
import graph falls back to the discovered source graph only for work units whose imports have not yet
been recovered.

Local proof-completion history remains intact. Each observed old/new change and its affected work
units is appended to SQLite as analysis-only provenance; the scheduler never reads that history.

State exposes `proof_complete`, `interface_current`, `dependencies_current`, `head_build_status`, and
`fully_certified` projections for each task. The dashboard reports locally completed proofs separately
from proofs fully certified against HEAD.

`formalize_graph.fingerprint_metrics` records initialized baselines, interface-preserving edits,
interface-changing edits, queued descendants, automatic successful rechecks, and fingerprint
failures.
