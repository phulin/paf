# Capability-package Steward architecture

**Status: authoritative implementation contract (Phase 4 cutover).**

Status: design for replacing repair sweeps and upstream requests

## Purpose

PAF needs one agent to own a mathematical problem across the files in which that problem actually
lives. File-local proof agents are good at leaf proofs, but they cannot reliably decide whether a
failure calls for a consumer-local argument, an earlier general lemma, a shared abstraction, a
statement correction, or external library work. Passing prose between proof review, upstream repair,
and downstream retry agents distributes that decision across agents that see different fragments of
the problem. Even correct local diagnoses can therefore produce repeated requests and retries with
no one responsible for the final repository state.

The Steward is the durable owner of a **capability package**: a coherent mathematical objective,
all consumers currently blocked on it, the files in which it may be implemented, and the evidence
needed to validate it. A Steward may inspect the whole repository, edit several reserved files,
create supporting lemmas, revise the package plan after Lean probes, delegate independent leaf work,
and integrate the result. Other agents contribute evidence or scoped commits to the package; they do
not negotiate ownership with one another.

This design replaces both the current read-only repair-sweep planner and the upstream
request/answer/targeted-retry subsystem. The proof-blocker ledger remains as observation data. The
ordinary discover, formalize, review, and prove stages remain the source of routine work and stage
acceptance predicates.

## Core invariants

1. One mathematical capability has one active root package and one active Steward.
2. The Steward is the only agent that may change package scope, plan, placement, dependencies,
   consumer acceptance, or terminal disposition.
3. A package may span several source files, but every writable path is exclusively reserved before
   an agent edits it.
4. Agents generally read repository-wide without read locks. Discovery additionally reserves its
   assigned chapter's mutation scope, preventing any package or ordinary agent from changing that
   chapter while discovery reads it. Other read dependencies are protected by interface digests
   checked before integration.
5. Every package agent edits a private fuse-overlay workspace. At the end of that agent turn, PAF
   scopes and imports its accepted delta into canonical source exactly like an ordinary agent run.
6. The package outcome is repository state, not an answer sent to another agent. A reusable result
   is complete only when its declaration exists, is placeholder-free, validates, and has passed the
   applicable consumer checks.
7. General results are placed at the earliest natural abstraction layer. Consumer-specific facts
   remain with their consumers. The Steward must search existing interfaces before adding either.
8. A package may be split or merged, but the state transition and transfer of consumers, locks, and
   dependencies are atomic.
9. Model reports are proposals and evidence. PAF alone grants leases, reserves paths, changes
   durable status, imports commits, and records acceptance.
10. A crash or expired agent cannot publish its uncollected overlay or create two owners for the
    same package.

## Conceptual model

The normal pipeline still attempts work locally:

```text
discover -> formalize -> review -> prove
```

A local proof failure first enters the proof-blocker ledger. PAF keeps it local when there is a
genuinely new executable route or a small source-local edit. It attaches the blocker to a capability
package when any of the following holds:

- the needed declaration belongs in another file;
- several declarations exhibit the same missing interface;
- a checked retry route has failed once;
- proof review identifies substantial multi-lemma work;
- the correct placement or statement is uncertain;
- a consumer is waiting on shared or external mathematical infrastructure;
- the failure is already a consumer of an active package.

The package then owns the structural problem:

```text
blocker evidence
      |
      v
capability package <--------- additional consumers
      |
      v
Steward investigates, plans, edits, and delegates
      |
      v
package validation and consumer acceptance
      |
      +---- complete: close satisfied consumers
      +---- split: create independent child packages
      +---- external: park with an exact unavailable capability
      +---- statement revision: retain explicit human-visible decision
```

There is no upstream answer followed by a downstream retry. The Steward either implements the
capability and validates its consumers, implements only a well-defined sub-capability and splits the
remainder, discovers that the work is consumer-local and edits there, or records why the package
cannot currently be completed.

## Durable entities

### CapabilityPackage

```text
CapabilityPackage
  id
  capability_key
  aliases
  title
  mathematical_objective
  textbook_refs
  status
  disposition
  steward_lease
  write_scope
  expansion_scope
  consumer_ids
  evidence_ids
  step_ids
  parent_package_id
  child_package_ids
  depends_on_package_ids
  relevant_read_interfaces
  plan_revision
  integrated_revision
  created_at, updated_at
```

`capability_key` identifies the mathematical interface, not a consumer failure. PAF derives an
initial key from the desired declaration/signature, principal constants in the residual goal, and
the proposed abstraction module. The Steward may add aliases or merge packages after inspection,
but it may not silently change a package's identity.

`write_scope` is the exact reserved set of files or subtrees. `expansion_scope` is a bounded set of
paths the Steward may request when investigation shows that a result belongs at a different layer.
Read access is repository-wide.

### PackageConsumer

```text
PackageConsumer
  id
  package_id
  work_unit_id
  path
  declaration
  stage
  residual_goal
  source_digest
  blocker_ids
  attempted_routes
  acceptance_contract
  status
  accepted_revision
  detached_package_id
```

Consumers are independent. A shared capability can satisfy one consumer while another reveals a
new local obstruction. The first closes; the second is detached into a new package or remains open
with new evidence. One difficult consumer never invalidates successful acceptance for the others.

### PackageStep

```text
PackageStep
  id
  package_id
  objective
  intended_declarations
  intended_paths
  depends_on_step_ids
  kind
  status
  assigned_worker_id
  commit_ids
  validation_contract
  remaining_gap
```

Step kinds include investigation, interface, supporting lemma, consumer integration, statement
revision, and validation. Steps describe mathematical deliverables rather than generic requests to
“try again.” A useful implementation plan normally contains many small dependency-ordered lemmas so
that weak workers receive bounded goals.

The plan is revisioned. The Steward may revise it after an elaboration probe disproves an expected
API, but completed clean declarations remain recorded as completed steps rather than being erased by
the replan.

### PackageEvidence

```text
PackageEvidence
  id
  package_id
  producer
  kind
  source_revision
  paths
  declarations
  payload
  digest
  created_at
```

Evidence includes residual goals, diagnostics, failed approaches, exact declaration searches,
review findings, Lean probes, validation output, commits, and external dependency references.
Evidence is append-only. A current plan may supersede an interpretation, but it does not rewrite
the historical observation.

### Lease and reservation records

```text
StewardLease
  package_id
  agent_id
  generation
  acquired_at
  heartbeat_at
  expires_at

PathReservation
  normalized_path
  mode
  package_id
  lease_generation
  acquired_at
```

The lease generation is a fencing token. Every package mutation and integration operation supplies
the generation it observed. Work from an expired generation is rejected even if its old process is
still alive.

## Creating and clustering packages

PAF should create packages conservatively from deterministic evidence. Two blockers attach to the
same package when they share a capability key or when several strong signals agree:

- the same proposed declaration and normalized signature;
- the same residual-goal head constants;
- the same missing instance, equivalence, or structure;
- overlapping exact declarations attempted by proof agents;
- the same natural owner module;
- a direct import or textbook dependency;
- an explicit alias established by a prior Steward decision.

Adjacency alone is insufficient. Two failures in neighboring chapters may require unrelated
mathematics. Semantic embeddings may suggest candidates for a Steward to inspect, but they should
not establish durable identity or ownership.

Initial scope is derived from the consumer files, exact proposed owner paths, and configured module
ownership. PAF never accepts an arbitrary model-supplied filesystem path without resolving it to a
known repository scope. If no safe writable owner can be derived, the package begins in
`investigating` with consumer files reserved only when editing them is already justified.

When a new blocker matches an active package, PAF attaches it as a consumer and wakes the existing
Steward. It does not launch another planner or ask the consumer agent to contact the owner.

## The Steward

The Steward is a long-lived strong-model agent attached to one package generation. It is not a
read-only sweep planner. Its job is to leave the mathematical cluster in the best coherent
repository state achievable within the package:

1. inspect every attached blocker and confirm that the declarations and residual goals are current;
2. read all relevant textbook sections and repository interfaces;
3. identify whether the missing work is existing, consumer-local, shared, statement-level, or
   external;
4. choose the natural declaration placement and request any necessary scope expansion;
5. write a dependency-ordered plan containing small lemmas and exact acceptance checks;
6. run focused Lean probes before committing to nontrivial compositions or universe choices;
7. implement central interfaces itself and delegate genuinely independent leaf steps when useful;
8. inspect and integrate worker commits;
9. update affected consumers and run package acceptance;
10. complete, split, merge, or park the package with exact remaining work.

The Steward may edit any reserved file, including multiple chapters and shared support modules. It
may correct an internal statement when the textbook or surrounding formalization shows that the
statement is wrong. Public statement changes must be explicit plan steps with recorded affected
consumers; they are never incidental elaboration cleanup.

The Steward must not:

- add a duplicate result instead of finding an existing declaration;
- place a general theorem in a late consumer merely because that file is already writable;
- bypass a missing abstraction with copied consumer-specific proofs;
- weaken validation, add placeholders, or hide warnings;
- edit an unreserved path;
- treat a changed error message as completion;
- delegate the placement decision or package lifecycle to a worker;
- send a prose request to another package and wait for an answer.

If another package owns required work, the Steward records a package dependency. PAF—not the two
agents—then attaches, merges, or orders the packages.

## Mathematical plan and source placement

The plan is both an implementation guide for the Steward and a roadmap from which weak workers can
take leaves. For every intended public declaration it records:

- a fully qualified candidate name and signature;
- the file in which the result belongs;
- why that location is the appropriate abstraction layer;
- exact existing declarations expected to discharge substantial steps;
- intermediate claims in dependency order;
- universe, typeclass, localization, completion, or coercion details likely to matter;
- a focused probe for the first nontrivial composition;
- the consumers expected to become checkable.

The Steward searches the selected chronological source prefix and Mathlib before introducing a
result. If an existing declaration suffices, the package plan records its use and proceeds directly
to consumer integration. If a new result has several consumers or is source-neutral, it belongs in
an earlier shared file. If it is merely an awkward specialization, the Steward should prove a
general helper and make the specialization a short corollary.

Source files should accumulate small named lemmas rather than giant conjunctions, opaque local
proofs, or repeated bridges. Package validation checks for newly duplicated declaration names and
for equivalent capability aliases already known to the registry. The Steward remains responsible
for mathematical duplication that cannot be detected syntactically.

## Delegated workers

Workers exist to execute bounded package steps, not to steer the package. A worker packet contains:

- one step objective and its intended declarations;
- exact writable paths, all already reserved by the parent package;
- relevant textbook excerpts and package evidence;
- completed prerequisite declarations and commits;
- known failed approaches;
- focused validation commands;
- the expected report and commit contract.

A worker cannot change placement, expand scope, split the package, attach consumers, or declare
package completion. If it discovers that the objective is wrongly placed or needs another file, it
returns evidence to the Steward without editing that path.

Workers run sequentially, each in a fresh private overlay based on canonical source. PAF imports and
commits one accepted scoped turn before acquiring the next worker's overlay. Agents never manipulate
Git branches or commits themselves, and two agents never edit the same overlay concurrently.

Worker failure does not create a peer request or a new review loop. Its report and validation become
package evidence. The Steward revises the step, handles it directly, or splits a genuinely
independent capability.

## Locking and isolation

### Why both semantic and path locks are required

Path reservations prevent Git conflicts but cannot stop two packages from independently adding the
same mathematical interface in different files. Capability ownership prevents duplicate work but
cannot stop unrelated packages from editing the same support file. PAF therefore maintains both:

- one active package reservation per canonical capability key or alias;
- exclusive path reservations for every file or subtree a package may write.

The package owns reservations; the current Steward holds a renewable lease over the package. A
replacement Steward inherits the package reservations after receiving a new fenced generation.

Every mutating PAF agent participates in this reservation system, not only package agents. An
ordinary formalize, review, or prove run receives an ephemeral reservation for its configured write
scope before it starts. It cannot start when that scope overlaps a package reservation, and a
package cannot acquire paths held by a running ordinary task. Existing per-work-unit locks become an
adapter over the same reservation table rather than a separate source of truth. Read-only discovery,
planning, inspection, and coordinator analysis do not reserve paths.

### Path modes

The initial implementation uses only exclusive reservations:

- `exclusive_file` for a source file;
- `exclusive_subtree` for creation or coordinated reorganization within a directory.

There is no shared-append mode. Lean files are ordered programs, and “independent” append operations
can still conflict through imports, namespaces, attributes, instances, and declaration order.

Paths are canonical repository-relative paths with symlinks resolved and case normalized where the
filesystem requires it. A file conflicts with a reservation on itself or any enclosing reserved
subtree. A subtree conflicts with every reservation beneath it.

### Atomic acquisition

PAF sorts requested paths canonically and grants the entire set atomically. If any path conflicts,
it grants none of the newly requested paths. Agents never acquire locks one at a time or block while
holding a partial new set, which prevents lock-order deadlocks.

An active package may retain its existing reservations while waiting for an expansion decision.
PAF does not let that wait continue indefinitely: it either records a package dependency, merges the
packages, queues the expansion behind a finishing package, or asks the Steward to checkpoint and
release paths on which it cannot currently progress.

### Scope expansion conflicts

When package A requests a path held by package B, PAF resolves the conflict in this order:

1. If the capabilities are equivalent, merge A and B under one package and one Steward.
2. If A needs an interface B is already implementing, attach A's consumer or add a package
   dependency on B.
3. If A can proceed against B's recorded intended interface, allow work on A's other reserved paths
   and revalidate after B integrates.
4. If the work is independent but physically colocated, queue A's expansion until B checkpoints and
   integrates.
5. If B's lease has expired, fence its agent, discard any uncollected overlay, and recover B before
   deciding whether its reservations can be released.

The scheduler changes the package graph. It never instructs the agents to negotiate ownership by
passing messages back and forth.

### Overlay agent workspaces

Each Steward turn and each bounded worker turn uses the same fuse-overlay workspace mechanism as an
ordinary PAF agent. The overlay is in-flight process state, not a durable package checkout. PAF
collects only reserved paths, performs stale-scope checks under the source barrier, imports the delta
into canonical source, creates the coordinator-owned commit, and closes the overlay. The next turn
starts from that canonical commit.

If a turn is interrupted before collection, its edits were never accepted and are discarded. Work
already collected at the end of an earlier turn is durable on the canonical branch.

### Read dependencies

Read access is unrestricted and does not acquire locks. Long-lived read locks would serialize the
formalization and make repository-wide investigation impossible. Instead, the package records
interface digests for declarations and modules on which its plan materially depends.

Before integration PAF recomputes those digests. Implementation-only changes that preserve the
recorded interface do not invalidate the package. A relevant interface change returns affected
steps to investigation or validation; it never permits a stale commit to publish automatically.

### Source barrier

Overlay collection and the coordinator-owned canonical commit occur under the same short source
barrier used by ordinary agents. Thinking, editing, and Lean validation do not hold that barrier.
Scoped manifest comparison rejects a stale overlay when another accepted turn changed its assigned
paths. Relevant interface digests are checked again before package or consumer acceptance.

### Lease expiry and fencing

The Steward heartbeats while it is inspecting, editing, waiting for workers, validating, and
integrating. Lease expiry makes the package eligible for recovery, but the fencing generation is
what makes recovery safe. Every later state write or integration from the expired agent is rejected.

Recovery increments the generation and assigns a new Steward. Any uncollected overlay belonged to
the expired agent and is discarded; accepted earlier turns are already canonical. Reservations
remain with the package during recovery so another package cannot overwrite its scope.

## Package dependencies, merging, and splitting

A package dependency means that one package requires the integrated interface of another. It is a
durable DAG edge, not a message. A dependent package may investigate and implement unrelated steps,
but consumer acceptance waits for the dependency revision it recorded.

Merging packages combines capability aliases, consumers, evidence, completed steps, dependencies,
and reservations in one atomic operation. Only one Steward lease survives; the other agent is
fenced, and any uncollected overlay is discarded.

Splitting creates child packages only when the remaining capabilities are independently placeable
and independently acceptable. Completed declarations stay with the package that owns their natural
interface. Consumers are explicitly reassigned. The parent becomes `decomposed` only after every
open consumer belongs to a live child or is otherwise terminally classified.

Package dependencies must remain acyclic. PAF rejects an edge that creates a cycle and asks the
Steward to merge the mutually dependent packages or design a lower common interface.

## Validation and acceptance

Validation is layered so that an upstream interface cannot be declared successful merely because
its own file compiles.

### Step validation

- intended declarations exist at their planned paths;
- new declarations are placeholder-free;
- the focused command or Lean probe passes;
- changed paths are within the worker's assigned subset;
- no forbidden configuration, validator, or unrelated source changes occurred.

### Package validation

- every modified file compiles independently;
- affected import dependents build against canonical source;
- package-level focused tests pass;
- new public interfaces match the plan or have an explicit revised plan;
- relevant textbook statements and dependency direction are preserved;
- no new placeholders or non-allowed warnings appear;
- known declaration and capability indexes show no accidental duplication.

### Consumer acceptance

Each consumer has its own acceptance contract. Normally PAF runs a focused check of the named
declaration on canonical source. A consumer is accepted when its original blocker is
gone and validation reaches the declaration without introducing a new package-owned obstruction.

If the shared interface validates but the consumer exposes unrelated local work, the capability is
still accepted for that consumer and the new work becomes a local blocker or another package. If the
consumer proves that the interface is insufficient, it remains attached with new evidence. Other
successful consumers stay closed.

A package is `complete` when all of its capability steps validate and every attached consumer is
accepted, detached, or terminally classified. It need not solve unrelated proofs discovered during
acceptance.

## Lifecycle

```text
observed
  -> investigating
  -> planned
  -> implementing
  -> validating
  -> integrating
  -> complete
```

Alternative durable states are:

- `waiting_dependency`: another package owns a required interface;
- `decomposed`: all remaining work and consumers moved to child packages;
- `external`: the exact capability belongs to an unavailable dependency;
- `statement_revision_required`: a public or textbook-level choice needs explicit authority;
- `parked`: current evidence is insufficient or safe scope cannot be acquired;
- `superseded`: merged into another package or made irrelevant by repository changes.

Operational interruption is not a mathematical status. A package with an interrupted agent remains
in its last durable lifecycle state and enters lease recovery.

State transitions are evidence-gated. A failed worker alone does not reopen local proof review; a
new source revision, plan revision, dependency integration, validated commit, or operator decision
does.

## Scheduling

The scheduler treats an active package as a long-lived job with internal steps, not as a fifth
linear stage. Ready package work competes with ordinary work using the downstream value of the
consumers it can release. The model may select a bounded effort class, but it cannot invent numeric
priority.

Only one Steward turn for a package runs at a time. Worker steps may run concurrently when their
dependencies are satisfied and their writable path subsets are disjoint. Package agents count
against the same global model and mutation capacity as ordinary agents; the integration/build queue
remains serialized where the underlying tools require it.

An ordinary task attached to a package is not repeatedly relaunched. Independent local blockers in
the same work unit may continue when their scopes do not conflict. When package integration changes
an interface, existing source-digest and import invalidation rules reopen exactly the affected
ordinary tasks.

## Replacing upstream requests

The package system removes the following concepts:

- `UpstreamRequestStatus` and persisted `upstream_requests`;
- proof and review report fields that create upstream requests;
- owner batching and upstream repair runs;
- upstream answer dispositions and declaration-answer validation;
- targeted downstream retries and their separate retry budgets;
- request escalation, reopening, and per-request repair/retry run ids.

Their useful data maps directly into packages:

| Existing upstream data                | Package representation                           |
| ------------------------------------- | ------------------------------------------------ |
| Request capability key                | Package capability key or alias                  |
| Blocked declaration and residual goal | Consumer and blocker evidence                    |
| Proposed owner                        | Initial placement hypothesis and expansion scope |
| Attempted alternatives                | Evidence ledger                                  |
| Acceptance tests                      | Consumer acceptance contract                     |
| Existing declaration answer           | Plan step using that declaration                 |
| Added declaration answer              | Validated package commit                         |
| Partial answer                        | Completed package steps plus remaining steps     |
| Decomposed answer                     | Child packages                                   |
| Downstream answer                     | Consumer-local placement decision                |
| External answer                       | `external` package disposition                   |
| Targeted retry                        | Consumer acceptance run                          |

Old persisted requests are read through a one-way compatibility importer that creates or attaches to
packages. They never re-enter the old request lifecycle. Once imported, package state is
authoritative.

Proof review also becomes smaller. It confirms current evidence and distinguishes local executable
work from structural work. Structural findings attach evidence to a package. Review does not select
an owner agent, reopen an unchanged proof, or conduct a separate request conversation.

## Prompt and report contracts

The Steward prompt supplies the complete package dossier rather than a sweep of unrelated failed
tasks. It includes current consumers, relevant textbook material, source and import graphs, exact
blockers, prior attempts, known capability aliases, reservations, dependency packages, completed
steps, and validation history.

The Steward's structured report describes mutations to the package model:

```text
diagnosis
placement_decision
scope_expansion_requests
plan_revision
completed_step_assessments
worker_assignments
package_dependency_requests
consumer_assessments
disposition
remaining_work
```

PAF validates all ids, paths, dependency edges, and lease generations. A report cannot itself grant
scope or publish an edit. Free-form explanation is retained as evidence but never parsed to drive a
state transition.

Worker reports are smaller: changed declarations, focused validation, exact remaining gap,
and newly discovered evidence. A worker cannot return ownership or lifecycle decisions.

## Persistence and recovery

Packages, consumers, steps, evidence, leases, reservations, and dependencies
belong in normalized durable tables. Runs continue to use the existing run history and token
accounting. Package snapshots in status output are derived views rather than the authoritative
record.

Every package mutation uses optimistic revision checking plus the lease fencing token. These
operations are transactional:

- attach a consumer;
- claim or renew a Steward lease;
- acquire or expand path reservations;
- revise a plan;
- assign a worker step;
- add a package dependency;
- merge or split packages;
- record consumer acceptance;
- finalize integration and release reservations.

Startup recovery handles fenced package state before assigning agents. It can recover:

- a lease with no live process;
- a package whose prior Steward lease expired;
- reservations or queued expansions fenced to the prior generation;
- a package whose relevant read interfaces changed while it was idle.

No recovery path infers mathematical success from an agent report or process exit code alone.

## Operator and UI model

The primary UI object is the package, not a collection of peer requests. It should show:

- capability, lifecycle state, Steward, lease generation, and heartbeat;
- consumers and their acceptance state;
- reserved and requested paths;
- plan steps, dependencies, assigned workers, and commits;
- package dependencies and downstream impact;
- latest validation evidence and integrated canonical revision;
- exact parked, external, or statement-revision reason;
- model usage, elapsed time, and cost for the package and its workers.

Operator actions act on packages: inspect, prioritize, park, resume, approve a public statement
revision, merge, split, transfer ownership, or release a quarantined reservation. An operator may
fence an agent and recover its package, but cannot mark capability or consumer acceptance without
the configured validation evidence.

## Acceptance criteria for the implementation

The Steward design is substantively complete when the following properties hold:

- two consumers of one missing result attach to one active package;
- one Steward can modify and validate several reserved files in an isolated overlay;
- a weak worker receives and completes a small dependency-ordered lemma without gaining package
  authority;
- conflicting path expansions create a merge, dependency, or queue decision without agent
  messaging or deadlock;
- an expired Steward cannot update state or integrate after a new generation is assigned;
- accepted turns survive restart as canonical commits while uncollected overlays are discarded;
- a package can close one consumer while splitting another consumer's new obstruction;
- shared declarations are placed in appropriate earlier files and are not repeated in consumers;
- old upstream requests import into packages and no new request/answer/retry loop is created;
- package integration invalidates and wakes only tasks affected by the resulting interface changes;
- every terminal package disposition is supported by current source, validation, dependency, or
  explicit operator evidence.
