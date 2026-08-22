# Incident-scoped escalation

PAF keeps the normal pipeline deterministic. Exceptional coordination is a separate control plane
for evidence that the ordinary task state machines cannot safely classify on their own. It does
not own the corpus, maintain a global plan, or continuously reconsider all outstanding work.

## Why incidents replace the global Steward

The old upstream Steward received every open and evaluating request whenever one new request
arrived. A malformed report, ambiguous owner, or unchanged `needs_scope` result could therefore
relaunch a large strong-model pass over the same ledger. It also mixed diagnosis, planning, editing,
and lifecycle recovery into one operation.

The escalation layer instead has three durable objects:

- A **signal** is canonical detector evidence with a stable identity and evidence digest.
- A **case** is a deterministic grouping of related signals, fenced by a generation.
- A **job** is one bounded agent invocation within a case generation.

Unchanged evidence does not open another generation. Evidence that arrives while an investigation
is running is recorded as pending; it does not cancel the active job. When that generation reaches
a terminal result, the pending digest opens a fresh generation with fresh budgets. This makes
no-progress suppression explicit while retaining new information.

## Deterministic detectors

The scheduler currently opens incidents for:

1. `upstream_request`: one or more live downstream observations grouped by normalized capability.
2. `source_issue`: a repeated, still-open source report. PAF checks whether the cited excerpt is
   present in the authoritative source and includes the source digest before an agent sees it.
3. `persistent_failure`: the same normalized failure signature in the configured number of recent
   non-auxiliary runs for one task.

Detector evidence is bounded. Trace incidents carry only the configured recent runs; the dossier
adds compact activity counters and at most 24 recent events for each selected run. Large residual
goals, excerpts, alternatives, and diagnostic text are truncated at deterministic limits.

## Cheap investigators and rare arbitration

Every incident first goes to a kind-specific investigator using the configured Luna profile:

- `owner_placement` checks exact APIs, chronology, and minimal write scope for an upstream request.
- `source_fact_check` distinguishes a real source defect from an omitted proof, stale citation, or
  Lean-only issue.
- `trace_diagnosis` compares recent probes and failures and requires materially new evidence before
  recommending a retry.

These jobs are read-only and run in disposable isolated workspaces. Their reports use one strict
schema and an allow-listed action. A high-confidence, deterministically valid local decision does
not invoke the planner.

The strong `escalation_coordinator` is a read-only arbiter. It is called only when the scout is
uncertain, asks for arbitration, proposes a source/public-interface change, returns an invalid
scope/action, or a failed repair generation explicitly forces reconsideration. It receives the
canonical signals and compact scout report, not the entire corpus or trace archive.

## Validated actions

Model reports never mutate workflow state directly. The scheduler validates the action enum, case
identity, configured work-unit ids, and kind-specific preconditions before applying one of these
transitions:

- `create_repair`: create one incremental upstream repair case. Its write scope must be nonempty,
  configured, and strictly earlier than every consumer; acceptance tests are mandatory.
- `retry_consumer` or `reject_observation`: resolve an upstream observation through the existing
  blocker/request transition logic.
- `retry_task`: requeue a persistent failure only when the report contains new evidence.
- `dismiss_source`: close a verified stale or invalid source report while retaining provenance.
- `propose_source_patch`: stop at `awaiting_source_approval`; textbook edits are never automatic.
- `park`: retain the incident and evidence without relaunching unchanged work.

Only the existing repair executor may edit source. It uses the configured cheap worker profile,
normal path isolation, validation, locking, and integration. A `needs_scope` or failed repair returns
to its incident under bounded scope-expansion/action-failure counters instead of starting a fresh
global Steward pass.

## Bounds and configuration

```toml
[escalation]
enabled = true
planner_model = "gpt-5.6-sol"
planner_reasoning_effort = "medium"
investigator_model = "gpt-5.6-luna"
investigator_reasoning_effort = "xhigh"
worker_model = "gpt-5.6-luna"
worker_reasoning_effort = "xhigh"
max_concurrent_investigations = 8
maximum_investigations_per_case = 4
maximum_planner_attempts = 2
maximum_scope_expansions = 2
source_issue_sighting_threshold = 2
persistent_failure_threshold = 3
recent_trace_runs = 3
```

Investigation capacity is separate from the normal worker pool. Transport or capacity failures may
retry only within the per-generation investigation budget. Semantic incompleteness parks the case.
Planner, scope-expansion, and post-repair failure counts are independently capped.

Set `enabled = false` only as a compatibility escape hatch; it restores the legacy global upstream
Steward route. Capability-package Steward execution remains disabled.

Use `paf incidents --config paf.toml` for a compact case table or add `--json` to inspect durable
case and job records. `paf source-issues` includes each issue's status and sighting count. Cases in
`awaiting_source_approval` or `parked` are deliberate operator boundaries, not hidden retries.
