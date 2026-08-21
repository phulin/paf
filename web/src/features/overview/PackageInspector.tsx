import { Boxes, GitBranch, KeyRound, Network, ShieldCheck, X } from "lucide-react";
import { Children, type ReactNode } from "react";
import { useMemo, useState } from "react";
import { IconButton } from "../../components/Controls";
import { timeAgo } from "../../lib/format";
import type { CapabilityPackage, SwarmState } from "../../types";

export function PackageInspector({ state, close }: { state: SwarmState; close: () => void }) {
  const packages = useMemo(
    () =>
      Object.values(state.capability_packages ?? {}).sort(
        (a, b) =>
          packageRank(a.status) - packageRank(b.status) || b.updated_at.localeCompare(a.updated_at),
      ),
    [state.capability_packages],
  );
  const [selectedId, setSelectedId] = useState(() => packages[0]?.id ?? "");
  const selected = packages.find((item) => item.id === selectedId) ?? packages[0] ?? null;

  return (
    <div
      className="drawer-backdrop"
      onMouseDown={(event) => event.target === event.currentTarget && close()}
    >
      <aside className="inspector-drawer package-inspector">
        <div className="drawer-header">
          <div>
            <span className="eyebrow">Durable capability ownership</span>
            <h2>Capability packages</h2>
          </div>
          <IconButton label="Close" onClick={close}>
            <X size={18} />
          </IconButton>
        </div>
        <div className="package-layout">
          <nav className="package-list" aria-label="Capability packages">
            {packages.map((item) => (
              <button
                className={item.id === selected?.id ? "selected" : ""}
                key={item.id}
                onClick={() => setSelectedId(item.id)}
              >
                <Boxes size={16} />
                <span>
                  <strong>{item.title}</strong>
                  <small>{item.capability_key}</small>
                </span>
                <em>{item.status}</em>
              </button>
            ))}
            {!packages.length && (
              <div className="agent-timeline-empty">
                <Boxes size={24} />
                <strong>No capability packages</strong>
                <span>Structural proof work will appear here when observed.</span>
              </div>
            )}
          </nav>
          {selected && <PackageDetail state={state} value={selected} select={setSelectedId} />}
        </div>
      </aside>
    </div>
  );
}

function PackageDetail({
  state,
  value,
  select,
}: {
  state: SwarmState;
  value: CapabilityPackage;
  select: (id: string) => void;
}) {
  const consumers = Object.values(state.package_consumers ?? {}).filter(
    (item) => item.package_id === value.id,
  );
  const steps = Object.values(state.package_steps ?? {}).filter(
    (item) => item.package_id === value.id,
  );
  const evidence = Object.values(state.package_evidence ?? {}).filter(
    (item) => item.package_id === value.id,
  );
  const reservations = Object.values(state.path_reservations ?? {}).filter(
    (item) => item.package_id === value.id,
  );
  const dependencies = (state.package_dependencies ?? []).filter(
    (item) => item.package_id === value.id,
  );
  const integrations = Object.values(state.integration_journal ?? {}).filter(
    (item) => item.package_id === value.id,
  );
  const interfaces = (state.relevant_read_interfaces ?? []).filter(
    (item) => item.package_id === value.id,
  );
  const children = Object.values(state.capability_packages ?? {}).filter(
    (item) => item.parent_package_id === value.id,
  );
  const lease = state.steward_leases?.[value.id];
  const completedSteps = steps.filter((item) => item.status === "complete").length;
  const openConsumers = consumers.filter((item) => item.status === "open").length;
  const latestIntegration = [...integrations].sort((a, b) =>
    b.updated_at.localeCompare(a.updated_at),
  )[0];
  const attention = packageAttention(
    value,
    lease?.agent_id,
    dependencies.length,
    latestIntegration?.phase,
  );
  return (
    <section className="package-detail">
      <header>
        <span className="status-chip">{value.status}</span>
        <h3>{value.title}</h3>
        <p>{value.mathematical_objective}</p>
        <small>
          revision {value.revision} · plan {value.plan_revision}
          {value.updated_at ? ` · ${timeAgo(value.updated_at)}` : ""}
        </small>
        {(value.base_revision || value.integrated_revision) && (
          <small>
            base {shortRevision(value.base_revision) || "unknown"}
            {value.integrated_revision
              ? ` · integrated ${shortRevision(value.integrated_revision)}`
              : " · not integrated"}
          </small>
        )}
      </header>
      <div className="package-attention">
        <span>Current handoff</span>
        <strong>{attention}</strong>
        <small>
          {completedSteps}/{steps.length} steps complete · {openConsumers} consumer
          {openConsumers === 1 ? "" : "s"} still open
        </small>
      </div>
      <div className="package-facts">
        <span>
          <strong>{consumers.length}</strong> consumers
        </span>
        <span>
          <strong>{steps.length}</strong> steps
        </span>
        <span>
          <strong>{evidence.length}</strong> evidence
        </span>
        <span>
          <strong>{reservations.length}</strong> reservations
        </span>
      </div>
      <PackageSection title="Steward lease" icon={<KeyRound size={14} />} empty="No active lease">
        {lease && (
          <article>
            <strong>{lease.agent_id}</strong>
            <span>
              fence {lease.generation} · acquired {timeAgo(lease.acquired_at)} · heartbeat{" "}
              {timeAgo(lease.heartbeat_at)} · expires {relativeDeadline(lease.expires_at)}
            </span>
          </article>
        )}
      </PackageSection>
      <PackageSection title="Consumers" empty="No consumers">
        {consumers.map((item) => (
          <article key={item.id}>
            <strong>{item.declaration || item.work_unit_id}</strong>
            <span>
              {item.status} · {item.stage} · {item.path}
            </span>
            <code>{item.residual_goal}</code>
            {item.accepted_revision && <p>accepted at {shortRevision(item.accepted_revision)}</p>}
            {!!item.blocker_ids.length && <p>blockers: {item.blocker_ids.join(", ")}</p>}
          </article>
        ))}
      </PackageSection>
      <PackageSection title="Plan steps" empty="No plan recorded">
        {steps.map((item) => (
          <article key={item.id}>
            <strong>{item.objective}</strong>
            <span>
              {item.kind} · {item.status}
              {item.assigned_worker_id ? ` · ${item.assigned_worker_id}` : ""}
            </span>
            <p>{item.intended_declarations.join(", ") || item.intended_paths.join(", ")}</p>
            {!!item.depends_on_step_ids.length && (
              <p>after: {item.depends_on_step_ids.join(", ")}</p>
            )}
            {!!item.commit_ids.length && (
              <p>commits: {item.commit_ids.map(shortRevision).join(", ")}</p>
            )}
            {item.remaining_gap && <code>gap: {item.remaining_gap}</code>}
            {!!Object.keys(item.validation_contract).length && (
              <code>acceptance: {compactJson(item.validation_contract)}</code>
            )}
          </article>
        ))}
      </PackageSection>
      <PackageSection title="Evidence" empty="No evidence">
        {evidence.map((item) => (
          <article key={item.id}>
            <strong>{item.kind}</strong>
            <span>
              {item.producer} · {item.created_at ? timeAgo(item.created_at) : ""}
            </span>
            <p>{item.declarations.join(", ") || item.paths.join(", ")}</p>
            {!!Object.keys(item.payload).length && <code>{compactJson(item.payload)}</code>}
          </article>
        ))}
      </PackageSection>
      <PackageSection
        title="Reservations and dependencies"
        icon={<GitBranch size={14} />}
        empty="No reservations or dependencies"
      >
        {reservations.map((item) => (
          <article key={item.normalized_path}>
            <strong>{item.normalized_path}</strong>
            <span>
              {item.mode} · generation {item.lease_generation}
            </span>
          </article>
        ))}
        {dependencies.map((item) => (
          <article key={item.depends_on_package_id}>
            <button className="package-link" onClick={() => select(item.depends_on_package_id)}>
              Depends on {packageName(state, item.depends_on_package_id)}
            </button>
            <span>{item.required_revision || "any accepted revision"}</span>
          </article>
        ))}
      </PackageSection>
      <PackageSection
        title="Ownership topology"
        icon={<Network size={14} />}
        empty="Standalone package"
      >
        {value.parent_package_id && (
          <article>
            <button className="package-link" onClick={() => select(value.parent_package_id!)}>
              Parent: {packageName(state, value.parent_package_id)}
            </button>
          </article>
        )}
        {children.map((item) => (
          <article key={item.id}>
            <button className="package-link" onClick={() => select(item.id)}>
              Child: {item.title}
            </button>
            <span>
              {item.status} · {item.capability_key}
            </span>
          </article>
        ))}
      </PackageSection>
      <PackageSection title="Read interfaces" empty="No external interface fingerprints">
        {interfaces.map((item) => (
          <article key={item.interface_id}>
            <strong>{item.interface_id}</strong>
            <span>
              {shortRevision(item.digest)} · source {shortRevision(item.source_revision)}
            </span>
          </article>
        ))}
      </PackageSection>
      <PackageSection title="Integration" empty="No integration attempt">
        {integrations.map((item) => (
          <article key={item.id}>
            <strong>{item.phase}</strong>
            <span>
              fence {item.lease_generation} ·{" "}
              {shortRevision(item.candidate_revision) || "candidate pending"} →{" "}
              {shortRevision(item.canonical_revision_after || item.canonical_revision_before)}
            </span>
            {item.validation_digest && <p>validation {shortRevision(item.validation_digest)}</p>}
            {!!item.provisional_consumer_ids.length && (
              <p>provisional: {item.provisional_consumer_ids.join(", ")}</p>
            )}
          </article>
        ))}
      </PackageSection>
      <PackageSection
        title="Placement and scope"
        icon={<ShieldCheck size={14} />}
        empty="No scope recorded"
      >
        {value.aliases.map((item) => (
          <article key={item}>
            <span>alias · {item}</span>
          </article>
        ))}
        {value.textbook_refs.map((item) => (
          <article key={item}>
            <strong>{item}</strong>
          </article>
        ))}
        {value.write_scope.map((item) => (
          <article key={item}>
            <strong>write · {item}</strong>
          </article>
        ))}
        {value.expansion_scope.map((item) => (
          <article key={item}>
            <span>may expand · {item}</span>
          </article>
        ))}
        {(value.branch || value.worktree) && (
          <article>
            <span>
              {value.branch} · {value.worktree}
            </span>
          </article>
        )}
      </PackageSection>
    </section>
  );
}

const TERMINAL = new Set([
  "complete",
  "decomposed",
  "external",
  "statement_revision_required",
  "parked",
  "superseded",
]);

function packageRank(status: string): number {
  if (status === "waiting_dependency" || status === "waiting_reservation") return 1;
  return TERMINAL.has(status) ? 2 : 0;
}

function packageAttention(
  value: CapabilityPackage,
  steward: string | undefined,
  dependencies: number,
  integrationPhase: string | undefined,
): string {
  if (value.status === "waiting_dependency")
    return `Blocked on ${dependencies} package ${dependencies === 1 ? "dependency" : "dependencies"}`;
  if (value.status === "waiting_reservation") return "Waiting for an overlapping path reservation";
  if (value.status === "integrating")
    return `Publishing validated work${integrationPhase ? ` · ${integrationPhase}` : ""}`;
  if (value.status === "validating") return "Checking the complete multi-file candidate";
  if (steward) return `${steward} owns the package worktree`;
  if (value.disposition) return `Closed as ${value.disposition}`;
  return "Ready for a Steward to claim";
}

function packageName(state: SwarmState, id: string): string {
  return state.capability_packages?.[id]?.title ?? id;
}

function shortRevision(value?: string | null): string {
  if (!value) return "";
  return value.length > 12 ? value.slice(0, 12) : value;
}

function compactJson(value: Record<string, unknown>): string {
  const encoded = JSON.stringify(value);
  return encoded.length > 240 ? `${encoded.slice(0, 237)}…` : encoded;
}

function relativeDeadline(timestamp: string): string {
  const seconds = Math.round((new Date(timestamp).getTime() - Date.now()) / 1000);
  if (seconds < 0) return `${timeAgo(timestamp)} (expired)`;
  if (seconds < 60) return `in ${seconds}s`;
  if (seconds < 3600) return `in ${Math.floor(seconds / 60)}m`;
  return `in ${Math.floor(seconds / 3600)}h`;
}

function PackageSection({
  title,
  icon,
  empty,
  children,
}: {
  title: string;
  icon?: ReactNode;
  empty: string;
  children: ReactNode;
}) {
  const items = Children.toArray(children);
  return (
    <div className="package-section">
      <h4>
        {icon}
        {title}
      </h4>
      {items.length ? children : <span className="muted">{empty}</span>}
    </div>
  );
}
