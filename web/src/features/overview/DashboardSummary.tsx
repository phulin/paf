import { CircleAlert, CheckCircle2, Users, Zap } from "lucide-react";
import type { ReactNode } from "react";
import { ProgressBar } from "../../components/Controls";
import { formatNumber } from "../../lib/format";
import type { Stage, SwarmState, Task } from "../../types";
import { STAGES, type ChapterRow } from "./model";

function MetricCard({
  icon,
  label,
  value,
  detail,
  accent,
  onClick,
}: {
  icon: ReactNode;
  label: string;
  value: string;
  detail: ReactNode;
  accent?: string;
  onClick?: () => void;
}) {
  const Component = onClick ? "button" : "div";
  return (
    <Component
      className={`metric-card${onClick ? " metric-card-action" : ""}`}
      style={{ "--accent": accent ?? "var(--cyan)" } as React.CSSProperties}
      onClick={onClick}
    >
      <div className="metric-icon">{icon}</div>
      <div className="metric-body">
        <span className="eyebrow">{label}</span>
        <div className="metric-value">{value}</div>
        <div className="metric-detail">{detail}</div>
      </div>
      <span className="corner-mark" />
    </Component>
  );
}

function StageCard({ stage, tasks }: { stage: Stage; tasks: Task[] }) {
  const succeeded = tasks.filter((task) => task.status === "succeeded").length;
  const running = tasks.filter((task) => task.status === "running").length;
  const queued = tasks.filter((task) => task.queued).length;
  const failed = tasks.filter(
    (task) =>
      task.status === "failed" ||
      task.status === "blocked" ||
      task.status === "interrupted" ||
      task.scheduling_status === "blocked",
  ).length;
  const percentage = tasks.length ? Math.round((100 * succeeded) / tasks.length) : 0;
  const stageColors: Record<Stage, string> = {
    discover: "var(--violet)",
    formalize: "var(--cyan)",
    review: "var(--amber)",
    prove: "var(--green)",
  };
  return (
    <div className="stage-card">
      <div className="stage-top">
        <div>
          <span className="stage-index">0{STAGES.indexOf(stage) + 1}</span>
          <span className="stage-name">{stage}</span>
        </div>
        <strong>{percentage}%</strong>
      </div>
      <ProgressBar value={percentage} color={stageColors[stage]} />
      <div className="stage-counts">
        <span className="success-text">✓ {succeeded}</span>
        <span className="running-text">▶ {running}</span>
        <span className="queued-text">· {queued}</span>
        <span className={failed ? "error-text" : "muted"}>! {failed}</span>
        <span className="muted">
          · {Math.max(0, tasks.length - succeeded - running - queued - failed)}
        </span>
      </div>
    </div>
  );
}

export function DashboardSummary({
  state,
  rows,
  connected,
  openIncidents,
}: {
  state: SwarmState;
  rows: ChapterRow[];
  connected: boolean;
  openIncidents: () => void;
}) {
  const tasks = Object.values(state.tasks ?? {});
  const successful = tasks.filter((task) => task.status === "succeeded").length;
  const chaptersDone = rows.filter(
    (row) => row.stages.prove?.proof_complete ?? row.stages.prove?.status === "succeeded",
  ).length;
  const headCertified = rows.filter((row) => row.stages.prove?.fully_certified).length;
  const runningTasks = tasks.filter((task) => task.status === "running").length;
  const activeAgents = state.agents?.active ?? runningTasks;
  const phasedTasks = tasks.some((task) => task.phase !== undefined);
  const postprocessing = phasedTasks
    ? tasks.filter((task) => task.status === "running" && task.phase === "postprocess").length
    : Math.max(0, runningTasks - activeAgents);
  const completion = tasks.length ? Math.round((100 * successful) / tasks.length) : 0;
  const incidents = Object.values(state.coordination_cases ?? {});
  const activeIncidents = incidents.filter((item) =>
    ["open", "running"].includes(item.status),
  ).length;
  const actionableIncidents = incidents.filter((item) => item.status === "actionable").length;
  const parkedIncidents = incidents.filter((item) => item.status === "parked").length;
  const operatorIncidents = incidents.filter((item) => item.operator_action_required).length;

  return (
    <>
      <div className="metrics-grid">
        <MetricCard
          icon={<Users size={19} />}
          label="Agents online"
          value={`${activeAgents} / ${state.agents?.maximum ?? 0}`}
          detail={
            <>
              <span className="success-text">{activeAgents} working</span>
              <i />
              {state.agents?.queued ?? 0} queued
              {postprocessing > 0 && (
                <>
                  <i />
                  {postprocessing} postprocessing
                </>
              )}
            </>
          }
          accent="var(--cyan)"
        />
        <MetricCard
          icon={<CheckCircle2 size={19} />}
          label="Pipeline progress"
          value={`${completion}%`}
          detail={
            <>
              <span>
                {successful} / {tasks.length} tasks
              </span>
              <i />
              {chaptersDone} locally proved
              <i />
              {headCertified} HEAD-certified
            </>
          }
          accent="var(--green)"
        />
        <MetricCard
          icon={<Zap size={19} />}
          label="Token ledger"
          value={formatNumber(state.usage?.total_tokens ?? 0)}
          detail={
            <>
              <span>{formatNumber(state.usage?.cached_input_tokens ?? 0)} cached</span>
              <i />${(state.cost?.estimated_usd ?? 0).toFixed(2)}
            </>
          }
          accent="var(--violet)"
        />
        <MetricCard
          icon={<CircleAlert size={19} />}
          label="Escalation incidents"
          value={`${activeIncidents} active`}
          detail={
            <>
              <span>{actionableIncidents} actionable</span>
              <i />
              {parkedIncidents} parked
              {operatorIncidents > 0 && (
                <>
                  <i />
                  <span className="error-text">{operatorIncidents} operator</span>
                </>
              )}
              {!connected && " · demo"}
            </>
          }
          accent={
            operatorIncidents || parkedIncidents
              ? "var(--amber)"
              : activeIncidents || actionableIncidents
                ? "var(--cyan)"
                : "var(--green)"
          }
          onClick={openIncidents}
        />
      </div>
      <section className="pipeline-strip">
        <div className="strip-label">
          <span className="eyebrow">Pipeline</span>
          <strong>Statement → proof</strong>
        </div>
        {STAGES.map((stage) => (
          <StageCard
            key={stage}
            stage={stage}
            tasks={tasks.filter((task) => task.stage === stage)}
          />
        ))}
      </section>
    </>
  );
}
