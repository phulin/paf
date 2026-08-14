import { AlertTriangle, CheckCircle2, Users, Zap } from "lucide-react";
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
}: {
  icon: ReactNode;
  label: string;
  value: string;
  detail: ReactNode;
  accent?: string;
}) {
  return (
    <div className="metric-card" style={{ "--accent": accent ?? "var(--cyan)" } as React.CSSProperties}>
      <div className="metric-icon">{icon}</div>
      <div className="metric-body"><span className="eyebrow">{label}</span><div className="metric-value">{value}</div><div className="metric-detail">{detail}</div></div>
      <span className="corner-mark" />
    </div>
  );
}

function StageCard({ stage, tasks }: { stage: Stage; tasks: Task[] }) {
  const succeeded = tasks.filter((task) => task.status === "succeeded").length;
  const running = tasks.filter((task) => task.status === "running").length;
  const failed = tasks.filter((task) => task.status === "failed" || task.status === "blocked").length;
  const percentage = tasks.length ? Math.round(100 * succeeded / tasks.length) : 0;
  const stageColors: Record<Stage, string> = {
    formalize: "var(--cyan)", fixup: "var(--violet)", review: "var(--amber)", prove: "var(--green)",
  };
  return (
    <div className="stage-card">
      <div className="stage-top"><div><span className="stage-index">0{STAGES.indexOf(stage) + 1}</span><span className="stage-name">{stage}</span></div><strong>{percentage}%</strong></div>
      <ProgressBar value={percentage} color={stageColors[stage]} />
      <div className="stage-counts">
        <span className="success-text">✓ {succeeded}</span><span className="running-text">▶ {running}</span>
        <span className={failed ? "error-text" : "muted"}>! {failed}</span><span className="muted">· {Math.max(0, tasks.length - succeeded - running - failed)}</span>
      </div>
    </div>
  );
}

export function DashboardSummary({
  state,
  rows,
  connected,
}: {
  state: SwarmState;
  rows: ChapterRow[];
  connected: boolean;
}) {
  const tasks = Object.values(state.tasks ?? {});
  const successful = tasks.filter((task) => task.status === "succeeded").length;
  const chaptersDone = rows.filter((row) => row.stages.prove?.status === "succeeded").length;
  const active = tasks.filter((task) => task.status === "running").length;
  const failed = tasks.filter((task) => task.status === "failed" || task.status === "blocked").length;
  const completion = tasks.length ? Math.round(100 * successful / tasks.length) : 0;

  return (
    <>
      <div className="metrics-grid">
        <MetricCard icon={<Users size={19} />} label="Agents online" value={`${state.agents?.active ?? active} / ${state.agents?.maximum ?? 0}`} detail={<><span className="success-text">{active} working</span><i />{state.agents?.queued ?? 0} queued</>} accent="var(--cyan)" />
        <MetricCard icon={<CheckCircle2 size={19} />} label="Pipeline progress" value={`${completion}%`} detail={<><span>{successful} / {tasks.length} tasks</span><i />{chaptersDone} chapters proved</>} accent="var(--green)" />
        <MetricCard icon={<Zap size={19} />} label="Token ledger" value={formatNumber(state.usage?.total_tokens ?? 0)} detail={<><span>{formatNumber(state.usage?.cached_input_tokens ?? 0)} cached</span><i />${(state.cost?.estimated_usd ?? 0).toFixed(2)}</>} accent="var(--violet)" />
        <MetricCard icon={<AlertTriangle size={19} />} label="Attention" value={String(failed)} detail={<><span>{failed ? "tasks need review" : "no systemic errors"}</span><i />{connected ? "live" : "demo"}</>} accent={failed ? "var(--red)" : "var(--amber)"} />
      </div>
      <section className="pipeline-strip">
        <div className="strip-label"><span className="eyebrow">Pipeline</span><strong>Statement → proof</strong></div>
        {STAGES.map((stage) => <StageCard key={stage} stage={stage} tasks={tasks.filter((task) => task.stage === stage)} />)}
      </section>
    </>
  );
}
