import { Activity, ArrowRight, Bot, ShieldCheck, X } from "lucide-react";
import { useMemo, useState } from "react";
import { IconButton } from "../../components/Controls";
import { timeAgo } from "../../lib/format";
import type { ShepherdAgent, ShepherdRun, SwarmState } from "../../types";
import { AgentTimelinePane } from "./ChapterInspector";

function agentKey(agent: ShepherdAgent): string {
  return agent.repair_work_unit_id || agent.run_id || `${agent.role}:${agent.work_unit_id}`;
}

function taskLocation(agent: ShepherdAgent): string {
  if (agent.location) return agent.location;
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
  const shepherd = state.shepherd;
  const runs: ShepherdRun[] = shepherd?.runs?.length
    ? shepherd.runs
    : [
        {
          id: shepherd?.current_sweep_id || "latest",
          status: shepherd?.status || "idle",
          trigger: "",
          failure_count: shepherd?.pending_failures ?? 0,
          started_at: shepherd?.last_started_at || "",
          finished_at: shepherd?.last_finished_at,
          summary: shepherd?.last_summary || "",
          error: shepherd?.last_error || "",
          agents: shepherd?.agents ?? [],
        },
      ];
  const currentRunIndex = runs.findIndex((run) => run.id === shepherd?.current_sweep_id);
  const initialRun = currentRunIndex >= 0 ? currentRunIndex : runs.length - 1;
  const [selectedRunId, setSelectedRunId] = useState(
    () => runs[initialRun]?.id ?? runs[runs.length - 1]?.id ?? "",
  );
  const selectedRun = runs.find((run) => run.id === selectedRunId) ?? runs[runs.length - 1];
  const agents = selectedRun?.agents ?? [];
  const initial = useMemo(
    () => agents.findIndex((agent) => agent.run_id && agent.status === "running"),
    [agents],
  );
  const [selectedKey, setSelectedKey] = useState(() =>
    agents[Math.max(0, initial)] ? agentKey(agents[Math.max(0, initial)]) : "",
  );
  const selected = agents.find((agent) => agentKey(agent) === selectedKey) ?? agents[0] ?? null;
  const activity = selected?.run_id ? state.activities?.[selected.run_id] : undefined;

  const selectRun = (run: ShepherdRun) => {
    setSelectedRunId(run.id);
    const running = run.agents.find((agent) => agent.run_id && agent.status === "running");
    setSelectedKey(running ? agentKey(running) : run.agents[0] ? agentKey(run.agents[0]) : "");
  };

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
            <span>
              <strong>${(shepherd?.cost?.estimated_usd ?? 0).toFixed(2)}</strong> total cost
            </span>
          </div>
          {(selectedRun?.error || selectedRun?.summary) && (
            <div className="drawer-section shepherd-summary">
              <span className="eyebrow">Selected run</span>
              <p>{selectedRun.error || selectedRun.summary}</p>
            </div>
          )}
          <div className="shepherd-run-tabs" role="tablist" aria-label="Shepherd runs">
            {runs.map((run, index) => (
              <button
                aria-selected={run.id === selectedRun?.id}
                className={run.id === selectedRun?.id ? "selected" : ""}
                key={run.id}
                onClick={() => selectRun(run)}
                role="tab"
              >
                <strong>Run {index + 1}</strong>
                <span>{run.status}</span>
              </button>
            ))}
          </div>
          <div className="drawer-section shepherd-agent-section">
            <span className="eyebrow">Agents · {selectedRun?.failure_count ?? 0} failures</span>
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
                        <strong title={location || agent.label}>{location || agent.label}</strong>
                        <small>{agent.label}</small>
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
