# Escalation

PAF handles ordinary work with deterministic scheduling and Luna workers. Escalation exists only
for evidence that does not fit a normal task transition.

The loop is deliberately small:

1. A deterministic detector fingerprints an anomaly and opens an incident.
2. One specialized Luna agent diagnoses that incident.
3. PAF validates its proposed action and hands executable work to the ordinary pipeline.
4. One read-only strong-model call is available when the Luna agent is uncertain or a repair fails.

Only signals and incidents are persisted. Agent history is already recorded by ordinary auxiliary
runs, so escalation has no separate job ledger, plan tree, lease system, or certificates.

## Detectors and cheap agents

PAF currently detects:

- live upstream requests, handled by `owner_placement`;
- repeated source reports, handled by `source_fact_check`; and
- identical recent task failures, handled by `trace_diagnosis`.

Each Luna agent receives only the related signals, configured work units, and a bounded summary of
recent traces. It is read-only. If its output violates the report contract, PAF gives the same Luna
thread one report-only correction attempt; malformed output does not invoke the strong model.

## Actions

The scheduler accepts a small action enum:

- `create_repair` creates a normal isolated repair assignment.
- `retry_consumer`, `reject_observation`, and `retry_task` use existing task and blocker transitions.
- `dismiss_source` closes a stale or invalid source report.
- `propose_source_patch` parks the incident for an operator; agents never edit the textbook.
- `park` records that unchanged work should not be relaunched.

For a repair, PAF validates configured scope ids and requires at least one earlier owner for each
consumer. The repair may also include its consumer for structural adaptation. Build commands are
derived from those work units rather than supplied as agent-authored acceptance evidence.
Adding or repairing a reusable Lean API in an earlier configured work unit is a normal repair, not
a source-patch proposal. Source-patch proposals are reserved for changing authoritative informal
source or the mathematical meaning of a stated theorem.

Upstream-request dossiers include the consumer and every configured work unit inferred from the
request's owner id and owner paths. The report also receives the incident-kind-specific action
allow-list. If a strong-model arbitration nevertheless violates that contract, PAF retains a valid
scout decision instead of turning an executable repair into a parked incident.

The incident statuses are `open`, `running`, `actionable`, `closed`, and `parked`. A changed evidence
digest reopens a closed or parked incident with a fresh attempt budget. Evidence arriving during a
run is retained for the next pass and prevents the stale result from mutating workflow state.

## Strong-model use and bounds

A high-confidence valid local action bypasses the strong model. One read-only arbitration is used
only for uncertainty, a source/public-interface proposal, or a failed/under-scoped repair. A
transport failure may retry within the incident budget; a completed arbitration is never repeated
for unchanged evidence.

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
maximum_attempts_per_incident = 4
source_issue_sighting_threshold = 2
persistent_failure_threshold = 3
recent_trace_runs = 3
```

`maximum_investigations_per_case` remains accepted as an older name for
`maximum_attempts_per_incident`. Setting `enabled = false` restores the legacy global upstream
Steward route as a compatibility escape hatch.

Use `paf incidents --config paf.toml` to inspect current incidents and outcomes. `paf source-issues`
shows source-report status and sighting counts.
