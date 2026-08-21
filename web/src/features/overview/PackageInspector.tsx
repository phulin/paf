import { Boxes, GitBranch, KeyRound, X } from "lucide-react";
import type { ReactNode } from "react";
import { useMemo, useState } from "react";
import { IconButton } from "../../components/Controls";
import { timeAgo } from "../../lib/format";
import type { CapabilityPackage, SwarmState } from "../../types";

export function PackageInspector({ state, close }: { state: SwarmState; close: () => void }) {
  const packages = useMemo(
    () =>
      Object.values(state.capability_packages ?? {}).sort((a, b) =>
        b.updated_at.localeCompare(a.updated_at),
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
          {selected && <PackageDetail state={state} value={selected} />}
        </div>
      </aside>
    </div>
  );
}

function PackageDetail({ state, value }: { state: SwarmState; value: CapabilityPackage }) {
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
  const lease = state.steward_leases?.[value.id];
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
      </header>
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
              generation {lease.generation} · heartbeat {timeAgo(lease.heartbeat_at)} · expires{" "}
              {timeAgo(lease.expires_at)}
            </span>
          </article>
        )}
      </PackageSection>
      <PackageSection title="Consumers" empty="No consumers">
        {consumers.map((item) => (
          <article key={item.id}>
            <strong>{item.declaration || item.work_unit_id}</strong>
            <span>
              {item.status} · {item.path}
            </span>
            <code>{item.residual_goal}</code>
          </article>
        ))}
      </PackageSection>
      <PackageSection title="Plan steps" empty="No plan recorded">
        {steps.map((item) => (
          <article key={item.id}>
            <strong>{item.title}</strong>
            <span>
              {item.kind} · {item.status}
              {item.assigned_agent_id ? ` · ${item.assigned_agent_id}` : ""}
            </span>
            <p>{item.objective}</p>
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
            <strong>Depends on {item.depends_on_package_id}</strong>
            <span>{item.required_revision || "any accepted revision"}</span>
          </article>
        ))}
      </PackageSection>
      <PackageSection title="Integration" empty="No integration attempt">
        {integrations.map((item) => (
          <article key={item.id}>
            <strong>{item.phase}</strong>
            <span>
              {item.candidate_revision || "candidate pending"} →{" "}
              {item.canonical_revision_after || item.canonical_revision_before}
            </span>
          </article>
        ))}
      </PackageSection>
    </section>
  );
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
  const items = Array.isArray(children) ? children.filter(Boolean) : children ? [children] : [];
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
