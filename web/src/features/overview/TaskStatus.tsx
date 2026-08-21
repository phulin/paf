import {
  AlertTriangle,
  CheckCircle2,
  CircleDashed,
  PauseCircle,
  Play,
  XCircle,
} from "lucide-react";
import { compactTaskDetail, timeAgo } from "../../lib/format";
import type { AgentActivity, SchedulingStatus, Task, TaskPhase, TaskStatus } from "../../types";

function StatusIcon({ status }: { status?: TaskStatus }) {
  switch (status) {
    case "succeeded":
      return <CheckCircle2 size={14} />;
    case "running":
      return <Play size={12} fill="currentColor" />;
    case "failed":
      return <XCircle size={14} />;
    case "blocked":
      return <AlertTriangle size={14} />;
    case "interrupted":
      return <PauseCircle size={14} />;
    default:
      return <CircleDashed size={14} />;
  }
}

export function StatusPill({
  status = "pending",
  rounds,
  building = false,
  queued = false,
  schedulingStatus,
  phase = "idle",
}: {
  status?: TaskStatus;
  rounds?: number;
  building?: boolean;
  queued?: boolean;
  schedulingStatus?: SchedulingStatus;
  phase?: TaskPhase;
}) {
  const effectiveStatus: TaskStatus =
    status === "pending" && schedulingStatus === "blocked" ? "blocked" : status;
  const displayStatus = building
    ? "building"
    : queued
      ? "queued"
      : status === "running" && phase === "postprocess"
        ? "postprocess"
        : effectiveStatus;
  return (
    <span className={`status-pill status-${displayStatus}`}>
      <StatusIcon status={effectiveStatus} />
      <span>
        {building
          ? "building"
          : queued
            ? "queued"
            : displayStatus === "postprocess"
              ? "postprocess"
              : effectiveStatus === "succeeded"
                ? "done"
                : effectiveStatus}
      </span>
      {Boolean(rounds) && <span className="round-count">×{rounds}</span>}
    </span>
  );
}

export function ActivityCell({ activity, task }: { activity?: AgentActivity; task?: Task }) {
  if (task?.status === "running" && task.phase === "postprocess") {
    return (
      <div className="activity-cell active postprocess">
        <span className="pulse-small" />
        <div>
          <strong>postprocessing agent result</strong>
          <span>{timeAgo(task.updated_at)}</span>
        </div>
      </div>
    );
  }
  if (activity?.current) {
    return (
      <div className="activity-cell active">
        <span className="pulse-small" />
        <div>
          <strong>{activity.current}</strong>
          <span>{timeAgo(activity.updated_at)}</span>
        </div>
      </div>
    );
  }
  if (task?.detail)
    return (
      <div className="activity-cell">
        <span className="muted">{compactTaskDetail(task.detail)}</span>
      </div>
    );
  return (
    <div className="activity-cell">
      <span className="muted">awaiting next stage</span>
    </div>
  );
}
