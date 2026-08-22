import { AlertTriangle, Bot, CircleAlert, Wrench, X } from "lucide-react";
import { useMemo, useState, type ReactNode } from "react";
import { IconButton } from "../../components/Controls";
import { timeAgo } from "../../lib/format";
import type {
  CoordinationCase,
  CoordinationDecision,
  CoordinationSignal,
  SwarmState,
} from "../../types";

export function IncidentInspector({ state, close }: { state: SwarmState; close: () => void }) {
  const incidents = useMemo(
    () =>
      Object.values(state.coordination_cases ?? {}).sort(
        (a, b) =>
          incidentRank(a.status) - incidentRank(b.status) ||
          (b.updated_at ?? "").localeCompare(a.updated_at ?? "") ||
          a.id.localeCompare(b.id),
      ),
    [state.coordination_cases],
  );
  const [selectedId, setSelectedId] = useState(() => incidents[0]?.id ?? "");
  const selected = incidents.find((item) => item.id === selectedId) ?? incidents[0] ?? null;

  return (
    <div
      className="drawer-backdrop"
      onMouseDown={(event) => event.target === event.currentTarget && close()}
    >
      <aside className="inspector-drawer incident-inspector">
        <div className="drawer-header">
          <div>
            <span className="eyebrow">Exceptional work only</span>
            <h2>Escalation incidents</h2>
          </div>
          <IconButton label="Close" onClick={close}>
            <X size={18} />
          </IconButton>
        </div>
        <div className="incident-layout">
          <nav className="incident-list" aria-label="Escalation incidents">
            {incidents.map((item) => (
              <button
                className={item.id === selected?.id ? "selected" : ""}
                key={item.id}
                onClick={() => setSelectedId(item.id)}
              >
                {item.operator_action_required ? (
                  <AlertTriangle size={16} />
                ) : (
                  <CircleAlert size={16} />
                )}
                <span>
                  <strong>{incidentKind(item.kind)}</strong>
                  <small>{item.work_unit_ids?.join(", ") || shortId(item.id)}</small>
                </span>
                <em>{item.status}</em>
              </button>
            ))}
            {!incidents.length && (
              <div className="agent-timeline-empty">
                <CircleAlert size={24} />
                <strong>No escalation incidents</strong>
                <span>Deterministic pipeline work is proceeding without an exceptional case.</span>
              </div>
            )}
          </nav>
          {selected && <IncidentDetail state={state} value={selected} />}
        </div>
      </aside>
    </div>
  );
}

function IncidentDetail({ state, value }: { state: SwarmState; value: CoordinationCase }) {
  const decision = value.decision ?? value.scout_report ?? null;
  const signals = (value.signal_ids ?? [])
    .map((id) => state.coordination_signals?.[id])
    .filter((item): item is CoordinationSignal => item !== undefined);
  const assignment = state.steward_cases?.[value.id];
  const runIds = Array.from(
    new Set([
      ...(value.coordination_run_ids ?? []),
      ...signals.flatMap(signalRunIds),
      ...(assignment?.repair_run_ids ?? []),
    ]),
  );
  const attempts = value.attempts ?? 0;
  const attemptLimit = value.maximum_attempts;

  return (
    <section className="incident-detail">
      <header>
        <div className="incident-heading-line">
          <span className={`status-chip incident-${value.status}`}>{value.status}</span>
          {value.operator_action_required && (
            <span className="operator-chip">
              <AlertTriangle size={12} /> operator action
            </span>
          )}
        </div>
        <h3>{incidentKind(value.kind)}</h3>
        <p>{value.work_unit_ids?.join(" · ") || "No work unit assigned"}</p>
        <small>
          generation {value.generation ?? 1} · {value.signal_ids?.length ?? 0} signal
          {(value.signal_ids?.length ?? 0) === 1 ? "" : "s"}
          {value.updated_at ? ` · ${timeAgo(value.updated_at)}` : ""}
        </small>
      </header>

      {value.operator_action_required && (
        <div className="incident-operator-callout">
          <AlertTriangle size={16} />
          <div>
            <strong>Source change proposed</strong>
            <span>
              This incident is parked for an operator decision; no source edit was applied.
            </span>
          </div>
        </div>
      )}

      <div className="incident-facts">
        <span>
          <strong>{attemptLimit ? `${attempts}/${attemptLimit}` : attempts}</strong> attempts
        </span>
        <span>
          <strong>{value.strong_used ? "strong used" : "Luna"}</strong> model tier
        </span>
        <span>
          <strong>{value.severity ?? "normal"}</strong> severity
        </span>
        <span>
          <strong>{runIds.length}</strong> related runs
        </span>
      </div>

      <div className="incident-attention">
        <span>Current outcome</span>
        <strong>{currentOutcome(value, decision)}</strong>
        {value.failure && <small>{value.failure}</small>}
      </div>

      <IncidentSection title="Decision" icon={<Bot size={14} />} empty="No decision yet">
        {decision && <DecisionDetail value={decision} />}
      </IncidentSection>

      {assignment && (
        <IncidentSection title="Repair scope" icon={<Wrench size={14} />}>
          <article>
            <strong>{assignment.title || "Focused repair"}</strong>
            <span>{assignment.status}</span>
            {assignment.needed_result && <p>{assignment.needed_result}</p>}
            {!!assignment.write_work_unit_ids?.length && (
              <code>write · {assignment.write_work_unit_ids.join(", ")}</code>
            )}
            {!!assignment.context_work_unit_ids?.length && (
              <code>read · {assignment.context_work_unit_ids.join(", ")}</code>
            )}
          </article>
        </IncidentSection>
      )}

      <IncidentSection title="Evidence" empty="No signal evidence retained">
        {signals.map((signal) => (
          <article key={signal.id}>
            <strong>{incidentKind(signal.kind)}</strong>
            <span>
              {signal.severity ?? "normal"} · {shortId(signal.id)}
            </span>
            <p>{signalSummary(signal)}</p>
          </article>
        ))}
      </IncidentSection>

      <IncidentSection title="Related runs" empty="No related run IDs retained">
        {runIds.map((runId) => {
          const activity = state.activities?.[runId];
          return (
            <article key={runId}>
              <strong>{shortId(runId)}</strong>
              <span>{activity?.current || activity?.latest_summary || "historical run"}</span>
            </article>
          );
        })}
      </IncidentSection>
    </section>
  );
}

function DecisionDetail({ value }: { value: CoordinationDecision }) {
  return (
    <article>
      <strong>{value.recommended_action?.replaceAll("_", " ") || "Recommendation pending"}</strong>
      <span>
        {[value.diagnosis, value.confidence && `${value.confidence} confidence`]
          .filter(Boolean)
          .join(" · ")}
      </span>
      {value.summary && <p>{value.summary}</p>}
      {value.objective && <code>objective · {value.objective}</code>}
      {value.rationale && <p>{value.rationale}</p>}
      {!!value.new_evidence?.length && <code>new evidence · {value.new_evidence.join("; ")}</code>}
    </article>
  );
}

function IncidentSection({
  title,
  icon,
  empty,
  children,
}: {
  title: string;
  icon?: ReactNode;
  empty?: string;
  children?: ReactNode;
}) {
  const populated = Array.isArray(children) ? children.length > 0 : Boolean(children);
  return (
    <section className="incident-section">
      <h4>
        {icon}
        {title}
      </h4>
      {populated ? children : <span className="muted">{empty}</span>}
    </section>
  );
}

function currentOutcome(value: CoordinationCase, decision: CoordinationDecision | null): string {
  if (value.operator_action_required) return "Waiting for an operator source decision";
  if (decision?.recommended_action) return decision.recommended_action.replaceAll("_", " ");
  if (value.status === "running") return "A bounded investigator is working";
  if (value.status === "open") return "Waiting for an investigator";
  if (value.failure) return "Parked after bounded investigation";
  return value.status;
}

function signalRunIds(signal: CoordinationSignal): string[] {
  const evidence = signal.evidence ?? {};
  return ["run_ids", "origin_run_ids"].flatMap((key) => stringArray(evidence[key]));
}

function signalSummary(signal: CoordinationSignal): string {
  const evidence = signal.evidence ?? {};
  for (const key of [
    "needed_result",
    "description",
    "failure_signature",
    "task_detail",
    "residual_goal",
    "location",
  ]) {
    const value = evidence[key];
    if (typeof value === "string" && value.trim()) return value;
  }
  return `Evidence ${signal.evidence_digest || "retained"}`;
}

function stringArray(value: unknown): string[] {
  return Array.isArray(value)
    ? value.filter((item): item is string => typeof item === "string")
    : [];
}

function incidentRank(status: string): number {
  if (status === "running") return 0;
  if (status === "open" || status === "actionable") return 1;
  if (status === "parked") return 2;
  return 3;
}

function incidentKind(kind: string): string {
  return (
    {
      upstream_request: "Owner placement",
      source_issue: "Source issue",
      persistent_failure: "Persistent failure",
    }[kind] ?? kind.replaceAll("_", " ")
  );
}

function shortId(value: string): string {
  return value.length > 28 ? `${value.slice(0, 25)}…` : value;
}
