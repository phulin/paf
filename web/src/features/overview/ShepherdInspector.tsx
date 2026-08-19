import { Activity, ArrowRight, Bot, ShieldCheck, X } from "lucide-react";
import { useMemo, useState } from "react";
import { IconButton } from "../../components/Controls";
import { timeAgo } from "../../lib/format";
import type { ShepherdAgent, SwarmState } from "../../types";
import { AgentTimelinePane } from "./ChapterInspector";

function agentKey(agent: ShepherdAgent): string {
  return agent.repair_work_unit_id || agent.run_id || `${agent.role}:${agent.work_unit_id}`;
}

function taskLocation(agent: ShepherdAgent): string {
  const document = agent.document_title || agent.document_id;
  const chapter = agent.ordinal == null ? "" : `Chapter ${agent.ordinal}`;
  const location = [document, chapter].filter(Boolean).join(" · ");
  if (location && agent.unit_title) return `${location} — ${agent.unit_title}`;
  return location || agent.unit_title || agent.work_unit_id;
}

export function ShepherdInspector({
  state,
  close,
  openAgent,
}: {
  state: SwarmState;
  close: () => void;
  openAgent: (agent: ShepherdAgent) => void;
}) {
  const agents = state.shepherd?.agents ?? [];
  const initial = useMemo(
    () => agents.findIndex((agent) => agent.run_id && agent.status === "running"),
    [agents],
  );
  const [selectedKey, setSelectedKey] = useState(() =>
    agents[Math.max(0, initial)] ? agentKey(agents[Math.max(0, initial)]) : "",
  );
  const selected = agents.find((agent) => agentKey(agent) === selectedKey) ?? agents[0] ?? null;
  const activity = selected?.run_id ? state.activities?.[selected.run_id] : undefined;
  const shepherd = state.shepherd;

  return (
    <div
      className="drawer-backdrop"
      onMouseDown={(event) => event.target === event.currentTarget && close()}
    >
      <aside className="inspector-drawer shepherd-inspector">
        <div className="agent-detail-pane">
          <div className="drawer-header">
            <div>
              <span className="eyebrow">Repair orchestration</span>
              <h2>Shepherd · {shepherd?.status ?? "off"}</h2>
            </div>
            <IconButton label="Close" onClick={close}>
              <X size={18} />
            </IconButton>
          </div>
          <div className="shepherd-summary-grid">
            <span>
              <strong>{shepherd?.pending_failures ?? 0}</strong> failures
            </span>
            <span>
              <strong>{shepherd?.running_units ?? 0}</strong> running
            </span>
            <span>
              <strong>{shepherd?.succeeded_units ?? 0}</strong> succeeded
            </span>
            <span>
              <strong>{shepherd?.failed_units ?? 0}</strong> failed
            </span>
          </div>
          {(shepherd?.last_error || shepherd?.last_summary) && (
            <div className="drawer-section shepherd-summary">
              <span className="eyebrow">Latest sweep</span>
              <p>{shepherd.last_error || shepherd.last_summary}</p>
            </div>
          )}
          <div className="drawer-section shepherd-agent-section">
            <span className="eyebrow">Relevant agents</span>
            <div className="shepherd-agent-list">
              {agents.length ? (
                agents.map((agent) => {
                  const active = agent === selected;
                  const location = taskLocation(agent);
                  return (
                    <button
                      className={`shepherd-agent-row${active ? " selected" : ""}`}
                      key={agentKey(agent)}
                      onClick={() => setSelectedKey(agentKey(agent))}
                    >
                      <span className="shepherd-agent-icon">
                        {agent.role === "shepherd" ? <ShieldCheck size={16} /> : <Bot size={16} />}
                      </span>
                      <span>
                        <strong title={location || agent.label}>
                          {agent.role === "repair_worker" && location ? location : agent.label}
                        </strong>
                        <small>{agent.role === "repair_worker" ? agent.label : location}</small>
                        <small title={agent.objective}>
                          {agent.objective || agent.work_unit_id || "Planning repair DAG"}
                        </small>
                      </span>
                      <em>{agent.status}</em>
                    </button>
                  );
                })
              ) : (
                <div className="agent-timeline-empty">
                  <Activity size={24} />
                  <strong>No Shepherd sweep recorded</strong>
                  <span>The planner and its repair workers will appear here.</span>
                </div>
              )}
            </div>
          </div>
          {selected && (
            <div className="drawer-section shepherd-selection">
              <span className="eyebrow">Selected agent</span>
              <strong>{activity?.current || selected.objective || selected.label}</strong>
              {taskLocation(selected) && (
                <span className="shepherd-selection-location">{taskLocation(selected)}</span>
              )}
              <span>
                {selected.stage} · {selected.status} ·{" "}
                {selected.run_id ? `run ${selected.run_id.slice(0, 12)}` : "not started"}
                {activity?.updated_at ? ` · ${timeAgo(activity.updated_at)}` : ""}
              </span>
              <button
                className="open-agent-button"
                disabled={!selected.run_id || !selected.work_unit_id}
                onClick={() => openAgent(selected)}
              >
                Open agent view <ArrowRight size={14} />
              </button>
            </div>
          )}
        </div>
        <AgentTimelinePane activity={activity} />
      </aside>
    </div>
  );
}
