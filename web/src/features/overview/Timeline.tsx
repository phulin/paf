import { Activity, AlertTriangle, Check, ChevronRight, Clock3, Cpu, TerminalSquare, Users, X } from "lucide-react";
import { useEffect } from "react";
import { IconButton, ProgressBar } from "../../components/Controls";
import { demoState } from "../../demo";
import { formatNumber, timeAgo } from "../../lib/format";
import type { ActivityEvent, SwarmState } from "../../types";

export function eventTime(event: ActivityEvent): string {
  try { return new Date(event.at).toLocaleTimeString([], { hour12: false, hour: "2-digit", minute: "2-digit", second: "2-digit" }); }
  catch { return "--:--:--"; }
}

export function EventKind({ kind }: { kind: string }) {
  const short: Record<string, string> = {
    mcp_tool_call: "mcp", file_change: "edit", command_execution: "bash", reasoning: "think",
    todo: "plan", message: "agent", error: "error", agent: "agent", usage: "tokens",
  };
  return <span className={`event-kind kind-${kind}`}>{short[kind] ?? kind.slice(0, 8)}</span>;
}

function LiveFeed({ state, drawer = false }: { state: SwarmState; drawer?: boolean }) {
  const events = Object.entries(state.activities ?? {})
    .flatMap(([runId, activity]) => (activity.recent ?? []).map((event) => ({ ...event, runId })))
    .sort((left, right) => right.at.localeCompare(left.at))
    .slice(0, drawer ? 60 : 7);
  return (
    <section className={`panel feed-panel ${drawer ? "drawer-feed" : ""}`}>
      {!drawer && <div className="panel-header"><div><span className="eyebrow">Event stream</span><h2>Agent timeline</h2></div><span className="tiny-live"><span /> tailing</span></div>}
      <div className="feed-list">
        {events.length ? events.map((event, index) => (
          <div className="feed-event" key={`${event.runId}-${event.sequence ?? index}`}>
            <time>{eventTime(event)}</time><div className="feed-line"><span className="feed-dot" /><span className="feed-rail" /></div>
            <div className="feed-content"><EventKind kind={event.kind} /><strong>{event.title}</strong>{event.detail && <p>{event.detail}</p>}</div>
          </div>
        )) : <div className="feed-empty"><Activity size={24} /><strong>No agent events right now</strong><span>The stream will update when a run starts.</span></div>}
      </div>
    </section>
  );
}

export function BuildPanel({ state, openTimeline }: { state: SwarmState; openTimeline: () => void }) {
  const build = state.coordinator_build ?? demoState.coordinator_build;
  const progress = build.total ? 100 * build.completed / build.total : 0;
  const eventCount = Object.values(state.activities ?? {}).reduce((total, activity) => total + (activity.recent?.length ?? 0), 0);
  return (
    <section className="panel build-panel">
      <div className="build-heading"><div><span className="eyebrow">Coordinator</span><h2>Build channel</h2></div><span className={`build-state ${build.active ? "active" : ""}`}>{build.active ? "building" : "idle"}</span></div>
      <div className="build-target"><div className="terminal-icon"><TerminalSquare size={18} /></div><div><span>{build.mode || "targeted"} build</span><strong>{build.current_chapter_id ?? "No chapter reserved"}</strong></div></div>
      <div className="build-progress-block">
        <div className="build-progress-label"><span>{build.completed ?? 0} / {build.total ?? 0} modules</span><strong>{Math.round(progress)}%</strong></div>
        <ProgressBar value={progress} color="var(--green)" />
        <div className="build-counters">
          <span><Check size={13} /> iter {build.iteration ?? 0}/{build.maximum_iterations ?? 0}</span>
          <span className={build.error_count ? "error-text" : ""}><X size={13} /> {build.error_count ?? 0} errors</span>
          <span className={build.warning_count ? "warning-text" : ""}><AlertTriangle size={13} /> {build.warning_count ?? 0} warnings</span>
        </div>
      </div>
      <div className="terminal-output">
        <div className="terminal-bar"><span /><span /><span /><em>lake build</em></div>
        {(build.output_tail?.length ? build.output_tail : ["coordinator build idle", "waiting for next certified change…"]).slice(-2).map((line, index) => <div key={index}><span className="prompt-mark">›</span>{line}</div>)}
        <span className="terminal-cursor" />
      </div>
      <button className="timeline-trigger" onClick={openTimeline}>
        <span className="timeline-trigger-icon"><Activity size={17} /></span><span><small>Event stream</small><strong>Agent timeline</strong></span><em>{formatNumber(eventCount)}</em><ChevronRight size={15} />
      </button>
    </section>
  );
}

export function TimelineDrawer({ state, close }: { state: SwarmState; close: () => void }) {
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => { if (event.key === "Escape") close(); };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [close]);

  return (
    <div className="drawer-backdrop timeline-backdrop" onMouseDown={(event) => event.target === event.currentTarget && close()}>
      <aside className="timeline-drawer">
        <div className="drawer-header timeline-drawer-header">
          <div><span className="eyebrow">Live event stream</span><h2>Agent timeline</h2></div>
          <div className="timeline-header-actions"><span className="tiny-live"><span /> tailing</span><IconButton label="Close" onClick={close}><X size={18} /></IconButton></div>
        </div>
        <div className="timeline-drawer-summary">
          <span><Users size={14} /><strong>{state.agents?.active ?? 0}</strong> agents</span>
          <span><Cpu size={14} /><strong>{Object.keys(state.activities ?? {}).length}</strong> runs</span>
          <span><Clock3 size={14} /> updated {timeAgo(state.updated_at)}</span>
        </div>
        <LiveFeed state={state} drawer />
      </aside>
    </div>
  );
}
