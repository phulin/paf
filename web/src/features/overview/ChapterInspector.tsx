import { Activity, Clock3, Cpu, FileCode2, TerminalSquare, X, XCircle } from "lucide-react";
import { IconButton } from "../../components/Controls";
import { compactTaskDetail, timeAgo } from "../../lib/format";
import type { AgentActivity } from "../../types";
import { AgentPlan, AgentUpdate, parseAgentReport } from "./AgentReport";
import { STAGES, type ChapterRow } from "./model";
import { StatusPill } from "./TaskStatus";
import { EventKind, eventTime } from "./Timeline";

function AgentTimelinePane({ activity }: { activity?: AgentActivity }) {
  const events = [...(activity?.recent ?? [])].sort((left, right) => left.at.localeCompare(right.at));
  const latestMessage = events.reduce<number | undefined>(
    (latest, event) => event.kind === "message" && event.sequence !== undefined ? Math.max(latest ?? event.sequence, event.sequence) : latest,
    undefined,
  );
  return (
    <section className="agent-timeline-pane">
      <div className="agent-timeline-header"><div><span className="eyebrow">Selected run</span><h2>Agent timeline</h2></div><span className="tiny-live"><span /> tailing</span></div>
      <div className="agent-timeline-meta">
        <span><TerminalSquare size={13} />{activity?.run_id?.slice(0, 12) ?? "no run"}</span><span><Activity size={13} />{events.length} events</span><span><Clock3 size={13} />{timeAgo(activity?.updated_at)}</span>
      </div>
      <div className="agent-event-list">
        {events.length > 0 ? events.map((event, index) => {
          const detail = event.kind === "message" && event.sequence === latestMessage ? activity?.latest_summary || event.detail : event.detail;
          const report = event.kind === "message" ? parseAgentReport(detail) : null;
          return (
            <div className={`agent-event status-${event.status}`} key={event.sequence ?? `${event.at}-${index}`}>
              <time>{eventTime(event)}</time><div className="agent-event-track"><span /><i /></div>
              <div className="agent-event-content"><div><EventKind kind={event.kind} /><em>{event.status}</em></div><strong>{event.title}</strong>{(report?.summary || detail) && <p>{report?.summary || detail}</p>}</div>
            </div>
          );
        }) : <div className="agent-timeline-empty"><Activity size={24} /><strong>No events recorded for this run</strong><span>The timeline will update when the agent emits activity.</span></div>}
      </div>
    </section>
  );
}

export function ChapterInspector({ row, close }: { row: ChapterRow; close: () => void }) {
  const activity = row.activity;
  return (
    <div className="drawer-backdrop" onMouseDown={(event) => event.target === event.currentTarget && close()}>
      <aside className="inspector-drawer">
        <div className="agent-detail-pane">
          <div className="drawer-header"><div><span className="eyebrow">{row.book} / chapter {String(row.number).padStart(2, "0")}</span><h2>{row.title}</h2></div><IconButton label="Close" onClick={close}><X size={18} /></IconButton></div>
          <div className="drawer-stage-list">
            {STAGES.map((stage, index) => {
              const task = row.stages[stage];
              return <div className="drawer-stage" key={stage}><span className="drawer-stage-number">0{index + 1}</span><div><strong>{stage}</strong><span>{compactTaskDetail(task?.detail) || (task?.status === "succeeded" ? `completed in ${task.rounds || 1} round` : "not started")}</span></div><StatusPill status={task?.status} rounds={task?.rounds} /></div>;
            })}
          </div>
          <div className="drawer-section"><AgentPlan activity={activity} /></div>
          <div className="drawer-section">
            <span className="eyebrow">Latest agent update</span><AgentUpdate activity={activity} />
            {activity && <div className="agent-stats"><span><TerminalSquare size={14} /> {activity.commands ?? 0} shell</span><span><Cpu size={14} /> {activity.mcp_calls ?? 0} MCP</span><span><FileCode2 size={14} /> {activity.file_changes ?? 0} edits</span><span><XCircle size={14} /> {activity.failures ?? 0} failures</span></div>}
          </div>
          <div className="drawer-section"><span className="eyebrow">Scope</span><code>lean/LastLib/{row.book.replace("book", "Book")}/Chapter{String(row.number).padStart(2, "0")}/**/*.lean</code></div>
        </div>
        <AgentTimelinePane activity={activity} />
      </aside>
    </div>
  );
}
