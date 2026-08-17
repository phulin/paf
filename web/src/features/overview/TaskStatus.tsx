import {
  AlertTriangle,
  CheckCircle2,
  CircleDashed,
  PauseCircle,
  Play,
  RotateCw,
  XCircle,
} from "lucide-react";
import { compactTaskDetail, timeAgo } from "../../lib/format";
import type { AgentActivity, Task, TaskPhase, TaskStatus } from "../../types";

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
  repairing = false,
  queued = false,
  phase = "idle",
}: {
  status?: TaskStatus;
  rounds?: number;
  building?: boolean;
  repairing?: boolean;
  queued?: boolean;
  phase?: TaskPhase;
}) {
  const displayStatus = repairing
    ? "repairing"
    : building
      ? "building"
      : queued
        ? "queued"
        : status === "running" && phase === "postprocess"
          ? "postprocess"
          : status;
  return (
    <span className={`status-pill status-${displayStatus}`}>
      {repairing ? <RotateCw size={13} /> : <StatusIcon status={status} />}
      <span>
        {repairing
          ? "repairing"
          : building
            ? "building"
            : queued
              ? "queued"
              : displayStatus === "postprocess"
                ? "postprocess"
                : status === "succeeded"
                  ? "done"
                  : status}
      </span>
      {Boolean(rounds) && <span className="round-count">×{rounds}</span>}
    </span>
  );
}

export function ActivityCell({ activity, task }: { activity?: AgentActivity; task?: Task }) {
  if (task?.repairing && !activity?.current) {
    return (
      <div className="activity-cell active">
        <span className="pulse-small" />
        <div>
          <strong>Shepherd repair in progress</strong>
          <span>{task.repair_work_unit_id || timeAgo(task.updated_at)}</span>
        </div>
      </div>
    );
  }
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
