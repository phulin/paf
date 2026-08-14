import { AlertTriangle, CheckCircle2, CircleDashed, Play, XCircle } from "lucide-react";
import { compactTaskDetail, timeAgo } from "../../lib/format";
import type { AgentActivity, Task, TaskStatus } from "../../types";

function StatusIcon({ status }: { status?: TaskStatus }) {
  switch (status) {
    case "succeeded": return <CheckCircle2 size={14} />;
    case "running": return <Play size={12} fill="currentColor" />;
    case "failed": return <XCircle size={14} />;
    case "blocked": return <AlertTriangle size={14} />;
    default: return <CircleDashed size={14} />;
  }
}

export function StatusPill({ status = "pending", rounds }: { status?: TaskStatus; rounds?: number }) {
  return (
    <span className={`status-pill status-${status}`}>
      <StatusIcon status={status} />
      <span>{status === "succeeded" ? "done" : status}</span>
      {Boolean(rounds) && <span className="round-count">×{rounds}</span>}
    </span>
  );
}

export function ActivityCell({ activity, task }: { activity?: AgentActivity; task?: Task }) {
  if (activity?.current) {
    return (
      <div className="activity-cell active">
        <span className="pulse-small" />
        <div><strong>{activity.current}</strong><span>{timeAgo(activity.updated_at)}</span></div>
      </div>
    );
  }
  if (task?.detail) return <div className="activity-cell"><span className="muted">{compactTaskDetail(task.detail)}</span></div>;
  return <div className="activity-cell"><span className="muted">awaiting next stage</span></div>;
}
