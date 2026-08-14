import {
  Activity,
  AlertTriangle,
  ArrowLeft,
  BookOpen,
  Box,
  Check,
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  CircleDashed,
  Clock3,
  Code2,
  Command,
  Copy,
  Cpu,
  Database,
  FileCode2,
  Filter,
  GitBranch,
  HardDrive,
  LayoutDashboard,
  ListFilter,
  Menu,
  Pause,
  Play,
  RefreshCw,
  Search,
  TerminalSquare,
  Users,
  X,
  XCircle,
  Zap,
} from "lucide-react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { demoState, demoStatementResponse } from "./demo";
import type {
  ActivityEvent,
  AgentActivity,
  DeclarationKind,
  LeanStatement,
  Stage,
  StatementResponse,
  SwarmSummary,
  SwarmState,
  Task,
  TaskStatus,
} from "./types";

type View = "overview" | "statements";
type ChapterRow = {
  id: string;
  book: string;
  number: number;
  title: string;
  stages: Partial<Record<Stage, Task>>;
  activity?: AgentActivity;
  latestTask?: Task;
};

const STAGES: Stage[] = ["formalize", "fixup", "review", "prove"];
const DECLARATION_KINDS: DeclarationKind[] = [
  "theorem",
  "lemma",
  "def",
  "abbrev",
  "structure",
  "class",
  "instance",
];

function formatNumber(value: number): string {
  if (!Number.isFinite(value)) return "—";
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}m`;
  if (value >= 1_000) return `${(value / 1_000).toFixed(1)}k`;
  return value.toLocaleString();
}

function timeAgo(timestamp?: string): string {
  if (!timestamp) return "never";
  const seconds = Math.max(0, Math.round((Date.now() - new Date(timestamp).getTime()) / 1000));
  if (seconds < 5) return "just now";
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return `${Math.floor(seconds / 86400)}d ago`;
}

function statusIcon(status?: TaskStatus) {
  switch (status) {
    case "succeeded": return <CheckCircle2 size={14} />;
    case "running": return <Play size={12} fill="currentColor" />;
    case "failed": return <XCircle size={14} />;
    case "blocked": return <AlertTriangle size={14} />;
    default: return <CircleDashed size={14} />;
  }
}

function StatusPill({ status = "pending", rounds }: { status?: TaskStatus; rounds?: number }) {
  return (
    <span className={`status-pill status-${status}`}>
      {statusIcon(status)}
      <span>{status === "succeeded" ? "done" : status}</span>
      {Boolean(rounds) && <span className="round-count">×{rounds}</span>}
    </span>
  );
}

function useSwarmState(live: boolean) {
  const [state, setState] = useState<SwarmState>(demoState);
  const [swarms, setSwarms] = useState<SwarmSummary[]>([]);
  const [selectedSwarm, setSelectedSwarm] = useState<string | null>(() =>
    window.localStorage.getItem("lastlib.selectedSwarm"),
  );
  const [connected, setConnected] = useState(false);
  const [fetching, setFetching] = useState(true);

  const refresh = useCallback(async () => {
    setFetching(true);
    try {
      const listResponse = await fetch("/api/swarms", { cache: "no-store" });
      if (!listResponse.ok) throw new Error(`swarm list endpoint returned ${listResponse.status}`);
      const list = (await listResponse.json() as { swarms: SwarmSummary[] }).swarms;
      setSwarms(list);
      const target = list.some((swarm) => swarm.id === selectedSwarm)
        ? selectedSwarm
        : list.find((swarm) => swarm.active)?.id ?? list[0]?.id ?? null;
      if (target !== selectedSwarm) setSelectedSwarm(target);
      const params = target ? `?swarm=${encodeURIComponent(target)}` : "";
      const response = await fetch(`/api/swarm${params}`, { cache: "no-store" });
      if (!response.ok) throw new Error(`state endpoint returned ${response.status}`);
      setState(await response.json() as SwarmState);
      setConnected(true);
    } catch {
      setConnected(false);
    } finally {
      setFetching(false);
    }
  }, [selectedSwarm]);

  const selectSwarm = useCallback((swarmId: string) => {
    window.localStorage.setItem("lastlib.selectedSwarm", swarmId);
    setSelectedSwarm(swarmId);
  }, []);

  useEffect(() => {
    void refresh();
    if (!live) return;
    const timer = window.setInterval(() => void refresh(), 3000);
    return () => window.clearInterval(timer);
  }, [live, refresh]);

  return { state, swarms, selectedSwarm, selectSwarm, connected, fetching, refresh };
}

function IconButton({
  label,
  children,
  onClick,
  active = false,
}: {
  label: string;
  children: React.ReactNode;
  onClick?: () => void;
  active?: boolean;
}) {
  return (
    <button className={`icon-button ${active ? "active" : ""}`} onClick={onClick} title={label} aria-label={label}>
      {children}
    </button>
  );
}

function Header({
  view,
  setView,
  live,
  setLive,
  connected,
  fetching,
  refresh,
  swarms,
  selectedSwarm,
  selectSwarm,
}: {
  view: View;
  setView: (view: View) => void;
  live: boolean;
  setLive: (live: boolean) => void;
  connected: boolean;
  fetching: boolean;
  refresh: () => Promise<void>;
  swarms: SwarmSummary[];
  selectedSwarm: string | null;
  selectSwarm: (swarmId: string) => void;
}) {
  return (
    <header className="app-header">
      <div className="brand" onClick={() => setView("overview")} role="button" tabIndex={0}>
        <div className="brand-glyph" aria-hidden="true"><span>λ</span></div>
        <div>
          <div className="brand-name">LASTLIB</div>
          <div className="brand-sub">FORMALIZATION OBSERVATORY</div>
        </div>
      </div>
      <SwarmSwitcher swarms={swarms} selectedSwarm={selectedSwarm} onSelect={selectSwarm} />
      <nav className="top-nav" aria-label="Primary navigation">
        <button className={view === "overview" ? "active" : ""} onClick={() => setView("overview")}>
          <LayoutDashboard size={14} /> Overview
        </button>
        <button className={view === "statements" ? "active" : ""} onClick={() => setView("statements")}>
          <Code2 size={15} /> Statement browser
        </button>
      </nav>
      <div className="header-actions">
        <div className={`connection ${connected ? "connected" : "demo"}`} title={connected ? "Reading live repository state" : "Repository API unavailable; showing a demo snapshot"}>
          <span className="connection-dot" />
          <span>{connected ? "repository" : "demo mode"}</span>
        </div>
        <button className={`live-control ${live ? "on" : ""}`} onClick={() => setLive(!live)}>
          {live ? <Pause size={12} /> : <Play size={12} />}
          {live ? "Live" : "Paused"}
        </button>
        <IconButton label="Refresh now" onClick={() => void refresh()}>
          <RefreshCw size={16} className={fetching ? "spinning" : ""} />
        </IconButton>
      </div>
    </header>
  );
}

function SwarmSwitcher({
  swarms,
  selectedSwarm,
  onSelect,
}: {
  swarms: SwarmSummary[];
  selectedSwarm: string | null;
  onSelect: (swarmId: string) => void;
}) {
  const [open, setOpen] = useState(false);
  const rootRef = useRef<HTMLDivElement>(null);
  const selected = swarms.find((swarm) => swarm.id === selectedSwarm) ?? swarms[0];
  const active = swarms.filter((swarm) => swarm.active);
  const recent = swarms.filter((swarm) => !swarm.active);

  useEffect(() => {
    const close = (event: MouseEvent) => {
      if (!rootRef.current?.contains(event.target as Node)) setOpen(false);
    };
    const escape = (event: KeyboardEvent) => {
      if (event.key === "Escape") setOpen(false);
    };
    document.addEventListener("mousedown", close);
    document.addEventListener("keydown", escape);
    return () => {
      document.removeEventListener("mousedown", close);
      document.removeEventListener("keydown", escape);
    };
  }, []);

  const choose = (swarmId: string) => {
    onSelect(swarmId);
    setOpen(false);
  };

  return (
    <div className="swarm-switcher" ref={rootRef}>
      <button className={`swarm-trigger ${open ? "open" : ""}`} onClick={() => setOpen(!open)} aria-expanded={open}>
        <span className={`swarm-status-dot ${selected?.active ? "active" : ""}`} />
        <span className="swarm-trigger-copy">
          <small>Watching swarm</small>
          <strong>{selected?.id ?? "demo-snapshot"}</strong>
        </span>
        {selected && <span className="swarm-agent-count">{selected.active_agents}<em>/{selected.maximum_agents}</em></span>}
        <ChevronDown size={14} />
      </button>
      {open && (
        <div className="swarm-menu">
          <div className="swarm-menu-head">
            <div><span className="eyebrow">Swarm processes</span><strong>{active.length} currently running</strong></div>
            <Activity size={16} />
          </div>
          {active.length > 0 && <div className="swarm-menu-label">Running now</div>}
          {active.map((swarm) => (
            <SwarmMenuItem key={swarm.id} swarm={swarm} selected={swarm.id === selected?.id} onSelect={choose} />
          ))}
          {recent.length > 0 && <div className="swarm-menu-label recent">Recent state</div>}
          {recent.map((swarm) => (
            <SwarmMenuItem key={swarm.id} swarm={swarm} selected={swarm.id === selected?.id} onSelect={choose} />
          ))}
          {!swarms.length && <div className="swarm-menu-empty">Repository API unavailable</div>}
        </div>
      )}
    </div>
  );
}

function SwarmMenuItem({
  swarm,
  selected,
  onSelect,
}: {
  swarm: SwarmSummary;
  selected: boolean;
  onSelect: (swarmId: string) => void;
}) {
  return (
    <button className={`swarm-menu-item ${selected ? "selected" : ""}`} onClick={() => onSelect(swarm.id)}>
      <span className={`swarm-item-mark ${swarm.active ? "active" : ""}`}>{swarm.active ? <Play size={10} fill="currentColor" /> : <Pause size={10} />}</span>
      <span className="swarm-item-copy">
        <strong>{swarm.id}</strong>
        <small>{swarm.book_count} books · {swarm.task_count} tasks · updated {timeAgo(swarm.updated_at)}</small>
      </span>
      <span className="swarm-item-agents">
        <strong>{swarm.active_agents}</strong>
        <small>agents</small>
      </span>
      {selected && <Check size={14} />}
    </button>
  );
}

function Rail({ view, setView }: { view: View; setView: (view: View) => void }) {
  return (
    <aside className="rail">
      <div className="rail-main">
        <IconButton label="Overview" active={view === "overview"} onClick={() => setView("overview")}>
          <LayoutDashboard size={19} />
        </IconButton>
        <IconButton label="Lean statements" active={view === "statements"} onClick={() => setView("statements")}>
          <FileCode2 size={19} />
        </IconButton>
        <div className="rail-divider" />
        <IconButton label="Agents"><Users size={19} /></IconButton>
        <IconButton label="Build queue"><TerminalSquare size={19} /></IconButton>
        <IconButton label="Dependency graph"><GitBranch size={19} /></IconButton>
      </div>
      <div className="rail-foot">
        <span className="lean-version">L4</span>
      </div>
    </aside>
  );
}

function MetricCard({
  icon,
  label,
  value,
  detail,
  accent,
}: {
  icon: React.ReactNode;
  label: string;
  value: string;
  detail: React.ReactNode;
  accent?: string;
}) {
  return (
    <div className="metric-card" style={{ "--accent": accent ?? "var(--cyan)" } as React.CSSProperties}>
      <div className="metric-icon">{icon}</div>
      <div className="metric-body">
        <span className="eyebrow">{label}</span>
        <div className="metric-value">{value}</div>
        <div className="metric-detail">{detail}</div>
      </div>
      <span className="corner-mark" />
    </div>
  );
}

function ProgressBar({ value, color }: { value: number; color?: string }) {
  return (
    <div className="progress-track">
      <div className="progress-fill" style={{ width: `${Math.min(100, Math.max(0, value))}%`, background: color }} />
    </div>
  );
}

function StageCard({ stage, tasks }: { stage: Stage; tasks: Task[] }) {
  const succeeded = tasks.filter((task) => task.status === "succeeded").length;
  const running = tasks.filter((task) => task.status === "running").length;
  const failed = tasks.filter((task) => task.status === "failed" || task.status === "blocked").length;
  const percentage = tasks.length ? Math.round(100 * succeeded / tasks.length) : 0;
  const stageColors: Record<Stage, string> = {
    formalize: "var(--cyan)",
    fixup: "var(--violet)",
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
        <span className={failed ? "error-text" : "muted"}>! {failed}</span>
        <span className="muted">· {Math.max(0, tasks.length - succeeded - running - failed)}</span>
      </div>
    </div>
  );
}

function chapterRows(state: SwarmState): ChapterRow[] {
  const rows = new Map<string, ChapterRow>();
  Object.values(state.tasks ?? {}).forEach((task) => {
    const row = rows.get(task.chapter_id) ?? {
      id: task.chapter_id,
      book: task.book_id,
      number: task.chapter_number,
      title: task.chapter_title,
      stages: {},
    };
    row.stages[task.stage] = task;
    if (!row.latestTask || task.updated_at > row.latestTask.updated_at) row.latestTask = task;
    if (task.latest_run_id && state.activities?.[task.latest_run_id]) {
      row.activity = state.activities[task.latest_run_id];
    }
    rows.set(task.chapter_id, row);
  });
  const bookSortKey = (bookId: string): [number, number | string] => {
    const match = /^book(\d+)$/i.exec(bookId);
    return match ? [0, Number(match[1])] : [1, bookId.toLocaleLowerCase()];
  };
  return [...rows.values()].sort((left, right) => {
    const leftBook = bookSortKey(left.book);
    const rightBook = bookSortKey(right.book);
    if (leftBook[0] !== rightBook[0]) return leftBook[0] - rightBook[0];
    const bookOrder = typeof leftBook[1] === "number" && typeof rightBook[1] === "number"
      ? leftBook[1] - rightBook[1]
      : String(leftBook[1]).localeCompare(String(rightBook[1]));
    return bookOrder || left.number - right.number;
  });
}

function chapterLabel(row: ChapterRow): string {
  const match = /^book(\d+)$/i.exec(row.book);
  return `${match ? Number(match[1]) : row.book}.${row.number}`;
}

function ActivityBadge({ activity, task }: { activity?: AgentActivity; task?: Task }) {
  if (activity?.current) {
    return (
      <div className="activity-cell active">
        <span className="pulse-small" />
        <div><strong>{activity.current}</strong><span>{timeAgo(activity.updated_at)}</span></div>
      </div>
    );
  }
  if (task?.detail) return <div className="activity-cell"><span className="muted">{task.detail}</span></div>;
  return <div className="activity-cell"><span className="muted">awaiting next stage</span></div>;
}

function TaskTable({
  rows,
  selected,
  setSelected,
}: {
  rows: ChapterRow[];
  selected: ChapterRow | null;
  setSelected: (row: ChapterRow | null) => void;
}) {
  const [filter, setFilter] = useState<"active" | "all" | "issues">("active");
  const [query, setQuery] = useState("");
  const visible = rows.filter((row) => {
    const tasks = Object.values(row.stages);
    const matchesQuery = `${row.book} ${row.title}`.toLowerCase().includes(query.toLowerCase());
    if (!matchesQuery) return false;
    if (filter === "issues") return tasks.some((task) => task?.status === "failed" || task?.status === "blocked");
    if (filter === "active") return tasks.some((task) => task?.status !== "succeeded");
    return true;
  });

  return (
    <section className="panel task-panel">
      <div className="panel-header table-header">
        <div>
          <span className="eyebrow">Chapter matrix</span>
          <h2>Formalization queue</h2>
        </div>
        <div className="table-tools">
          <div className="mini-tabs">
            {(["active", "all", "issues"] as const).map((option) => (
              <button key={option} className={filter === option ? "active" : ""} onClick={() => setFilter(option)}>{option}</button>
            ))}
          </div>
          <label className="compact-search">
            <Search size={14} />
            <input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="filter chapters" />
          </label>
        </div>
      </div>
      <div className="table-scroll">
        <table className="task-table">
          <thead>
            <tr>
              <th>Book / chapter</th>
              {STAGES.map((stage) => <th key={stage}>{stage}</th>)}
              <th>Current agent activity</th>
              <th aria-label="Inspect" />
            </tr>
          </thead>
          <tbody>
            {visible.slice(0, 22).map((row) => (
              <tr key={row.id} className={selected?.id === row.id ? "selected" : ""} onClick={() => setSelected(row)}>
                <td>
                  <div className="chapter-cell">
                    <span className="chapter-index">{chapterLabel(row)}</span>
                    <div><strong>{row.title}</strong></div>
                  </div>
                </td>
                {STAGES.map((stage) => (
                  <td key={stage}><StatusPill status={row.stages[stage]?.status} rounds={row.stages[stage]?.rounds} /></td>
                ))}
                <td><ActivityBadge activity={row.activity} task={row.latestTask} /></td>
                <td><ChevronRight className="row-chevron" size={16} /></td>
              </tr>
            ))}
          </tbody>
        </table>
        {!visible.length && (
          <div className="empty-table"><ListFilter size={22} /><span>No chapters match this view.</span></div>
        )}
      </div>
      <div className="table-footer">
        <span>Showing {Math.min(visible.length, 22)} of {visible.length} chapters</span>
        <span>Click a row to inspect agent state</span>
      </div>
    </section>
  );
}

function eventTime(event: ActivityEvent): string {
  try { return new Date(event.at).toLocaleTimeString([], { hour12: false, hour: "2-digit", minute: "2-digit", second: "2-digit" }); }
  catch { return "--:--:--"; }
}

function EventKind({ kind }: { kind: string }) {
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
      {!drawer && <div className="panel-header">
          <div><span className="eyebrow">Event stream</span><h2>Agent timeline</h2></div>
          <span className="tiny-live"><span /> tailing</span>
        </div>}
      <div className="feed-list">
        {events.length ? events.map((event, index) => (
          <div className="feed-event" key={`${event.runId}-${event.sequence ?? index}`}>
            <time>{eventTime(event)}</time>
            <div className="feed-line"><span className="feed-dot" /><span className="feed-rail" /></div>
            <div className="feed-content">
              <EventKind kind={event.kind} />
              <strong>{event.title}</strong>
              {event.detail && <p>{event.detail}</p>}
            </div>
          </div>
        )) : (
          <div className="feed-empty"><Activity size={24} /><strong>No agent events right now</strong><span>The stream will update when a run starts.</span></div>
        )}
      </div>
    </section>
  );
}

function BuildPanel({ state, openTimeline }: { state: SwarmState; openTimeline: () => void }) {
  const build = state.coordinator_build ?? demoState.coordinator_build;
  const progress = build.total ? 100 * build.completed / build.total : 0;
  const eventCount = Object.values(state.activities ?? {}).reduce(
    (total, activity) => total + (activity.recent?.length ?? 0),
    0,
  );
  return (
    <section className="panel build-panel">
      <div className="build-heading">
        <div><span className="eyebrow">Coordinator</span><h2>Build channel</h2></div>
        <span className={`build-state ${build.active ? "active" : ""}`}>{build.active ? "building" : "idle"}</span>
      </div>
      <div className="build-target">
        <div className="terminal-icon"><TerminalSquare size={18} /></div>
        <div>
          <span>{build.mode || "targeted"} build</span>
          <strong>{build.current_chapter_id ?? "No chapter reserved"}</strong>
        </div>
      </div>
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
        {(build.output_tail?.length ? build.output_tail : ["coordinator build idle", "waiting for next certified change…"]).slice(-2).map((line, index) => (
          <div key={index}><span className="prompt-mark">›</span>{line}</div>
        ))}
        <span className="terminal-cursor" />
      </div>
      <button className="timeline-trigger" onClick={openTimeline}>
        <span className="timeline-trigger-icon"><Activity size={17} /></span>
        <span><small>Event stream</small><strong>Agent timeline</strong></span>
        <em>{formatNumber(eventCount)}</em>
        <ChevronRight size={15} />
      </button>
    </section>
  );
}

function TimelineDrawer({ state, close }: { state: SwarmState; close: () => void }) {
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") close();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [close]);

  return (
    <div className="drawer-backdrop timeline-backdrop" onMouseDown={(event) => event.target === event.currentTarget && close()}>
      <aside className="timeline-drawer">
        <div className="drawer-header timeline-drawer-header">
          <div><span className="eyebrow">Live event stream</span><h2>Agent timeline</h2></div>
          <div className="timeline-header-actions">
            <span className="tiny-live"><span /> tailing</span>
            <IconButton label="Close" onClick={close}><X size={18} /></IconButton>
          </div>
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

interface StructuredAgentReport {
  changed: boolean;
  complete: boolean;
  summary: string;
  issues: string[];
  fixupFindings: Array<{ description: string; ownerPaths: string[] }>;
  sourceIssues: Array<{
    location: string;
    sourceExcerpt: string;
    description: string;
    suggestedCorrection: string;
  }>;
}

function parseAgentReport(value?: string): StructuredAgentReport | null {
  if (!value) return null;
  try {
    const report = JSON.parse(value) as Record<string, unknown>;
    if (typeof report.summary !== "string") return null;
    const strings = (item: unknown) => Array.isArray(item)
      ? item.filter((entry): entry is string => typeof entry === "string")
      : [];
    const fixupFindings = Array.isArray(report.fixup_findings)
      ? report.fixup_findings.flatMap((item) => {
          if (!item || typeof item !== "object") return [];
          const finding = item as Record<string, unknown>;
          if (typeof finding.description !== "string") return [];
          return [{ description: finding.description, ownerPaths: strings(finding.owner_paths) }];
        })
      : [];
    const sourceIssues = Array.isArray(report.source_issues)
      ? report.source_issues.flatMap((item) => {
          if (!item || typeof item !== "object") return [];
          const issue = item as Record<string, unknown>;
          if (typeof issue.description !== "string") return [];
          return [{
            location: typeof issue.location === "string" ? issue.location : "Source location not supplied",
            sourceExcerpt: typeof issue.source_excerpt === "string" ? issue.source_excerpt : "",
            description: issue.description,
            suggestedCorrection: typeof issue.suggested_correction === "string" ? issue.suggested_correction : "",
          }];
        })
      : [];
    return {
      changed: report.changed === true,
      complete: report.complete === true,
      summary: report.summary,
      issues: strings(report.issues),
      fixupFindings,
      sourceIssues,
    };
  } catch {
    return null;
  }
}

function AgentUpdate({ activity }: { activity?: AgentActivity }) {
  const value = activity?.latest_summary;
  const report = parseAgentReport(value);
  if (!report) {
    return (
      <div className="agent-report plain">
        <p className="agent-summary">{value || activity?.current || "No agent update recorded for this chapter."}</p>
      </div>
    );
  }
  const findingCount = report.issues.length + report.fixupFindings.length + report.sourceIssues.length;
  return (
    <div className="agent-report">
      <div className="agent-report-status">
        <span className={`report-badge ${report.complete ? "complete" : "working"}`}>
          {report.complete ? <CheckCircle2 size={12} /> : <Activity size={12} />}
          {report.complete ? "complete" : "in progress"}
        </span>
        <span className={`report-badge ${report.changed ? "changed" : "unchanged"}`}>
          {report.changed ? <FileCode2 size={12} /> : <Check size={12} />}
          {report.changed ? "files changed" : "no changes"}
        </span>
      </div>
      <p className="agent-summary">{report.summary}</p>
      {findingCount === 0 && (
        <div className="report-clear"><CheckCircle2 size={13} /><span>No issues or follow-up findings reported</span></div>
      )}
      {report.issues.length > 0 && (
        <div className="report-group issues">
          <div className="report-group-title"><AlertTriangle size={13} /><span>Issues</span><em>{report.issues.length}</em></div>
          <ul>{report.issues.map((issue, index) => <li key={index}>{issue}</li>)}</ul>
        </div>
      )}
      {report.fixupFindings.length > 0 && (
        <div className="report-group fixups">
          <div className="report-group-title"><GitBranch size={13} /><span>Fixup findings</span><em>{report.fixupFindings.length}</em></div>
          {report.fixupFindings.map((finding, index) => (
            <div className="report-finding" key={index}>
              <p>{finding.description}</p>
              {finding.ownerPaths.length > 0 && <div className="owner-paths">{finding.ownerPaths.map((path) => <code key={path}>{path}</code>)}</div>}
            </div>
          ))}
        </div>
      )}
      {report.sourceIssues.length > 0 && (
        <div className="report-group source-issues">
          <div className="report-group-title"><BookOpen size={13} /><span>Source issues</span><em>{report.sourceIssues.length}</em></div>
          {report.sourceIssues.map((issue, index) => (
            <details className="source-issue" key={index}>
              <summary><span>{issue.location}</span><ChevronDown size={13} /></summary>
              <p>{issue.description}</p>
              {issue.sourceExcerpt && <blockquote>{issue.sourceExcerpt}</blockquote>}
              {issue.suggestedCorrection && <div className="suggested-fix"><strong>Suggested correction</strong><p>{issue.suggestedCorrection}</p></div>}
            </details>
          ))}
        </div>
      )}
    </div>
  );
}

function AgentPlan({ activity }: { activity?: AgentActivity }) {
  const todos = activity?.todos ?? [];
  const completed = todos.filter((todo) => todo.completed).length;
  const total = activity?.todo_total ?? todos.length;
  const progress = total > 0 ? Math.min(100, Math.round(((activity?.todo_completed ?? completed) / total) * 100)) : 0;
  const currentIndex = todos.findIndex((todo) => !todo.completed);

  return (
    <div className="agent-plan">
      <div className="agent-plan-heading">
        <span className="eyebrow">Current agent plan</span>
        {total > 0 && <em>{activity?.todo_completed ?? completed}/{total}</em>}
      </div>
      {todos.length > 0 ? (
        <>
          <div className="agent-plan-progress"><span style={{ width: `${progress}%` }} /></div>
          <ol>
            {todos.map((todo, index) => (
              <li className={`${todo.completed ? "completed" : ""} ${index === currentIndex ? "current" : ""}`} key={`${todo.text}-${index}`}>
                <span className="plan-marker">
                  {todo.completed ? <Check size={12} /> : <span>{String(index + 1).padStart(2, "0")}</span>}
                </span>
                <span>{todo.text}</span>
              </li>
            ))}
          </ol>
        </>
      ) : (
        <p className="agent-plan-empty">No plan has been published for this run yet.</p>
      )}
    </div>
  );
}

function ChapterInspector({ row, close }: { row: ChapterRow; close: () => void }) {
  const activity = row.activity;
  return (
    <div className="drawer-backdrop" onMouseDown={(event) => event.target === event.currentTarget && close()}>
      <aside className="inspector-drawer">
        <div className="drawer-header">
          <div><span className="eyebrow">{row.book} / chapter {String(row.number).padStart(2, "0")}</span><h2>{row.title}</h2></div>
          <IconButton label="Close" onClick={close}><X size={18} /></IconButton>
        </div>
        <div className="drawer-stage-list">
          {STAGES.map((stage, index) => {
            const task = row.stages[stage];
            return (
              <div className="drawer-stage" key={stage}>
                <span className="drawer-stage-number">0{index + 1}</span>
                <div><strong>{stage}</strong><span>{task?.detail || (task?.status === "succeeded" ? `completed in ${task.rounds || 1} round` : "not started")}</span></div>
                <StatusPill status={task?.status} rounds={task?.rounds} />
              </div>
            );
          })}
        </div>
        <div className="drawer-section">
          <AgentPlan activity={activity} />
        </div>
        <div className="drawer-section">
          <span className="eyebrow">Latest agent update</span>
          <AgentUpdate activity={activity} />
          {activity && (
            <div className="agent-stats">
              <span><TerminalSquare size={14} /> {activity.commands ?? 0} shell</span>
              <span><Cpu size={14} /> {activity.mcp_calls ?? 0} MCP</span>
              <span><FileCode2 size={14} /> {activity.file_changes ?? 0} edits</span>
              <span><XCircle size={14} /> {activity.failures ?? 0} failures</span>
            </div>
          )}
        </div>
        <div className="drawer-section">
          <span className="eyebrow">Scope</span>
          <code>lean/LastLib/{row.book.replace("book", "Book")}/Chapter{String(row.number).padStart(2, "0")}/**/*.lean</code>
        </div>
      </aside>
    </div>
  );
}

function Overview({ state, connected }: { state: SwarmState; connected: boolean }) {
  const rows = useMemo(() => chapterRows(state), [state]);
  const [selected, setSelected] = useState<ChapterRow | null>(null);
  const [timelineOpen, setTimelineOpen] = useState(false);
  const tasks = Object.values(state.tasks ?? {});
  const successful = tasks.filter((task) => task.status === "succeeded").length;
  const chaptersDone = rows.filter((row) => row.stages.prove?.status === "succeeded").length;
  const active = tasks.filter((task) => task.status === "running").length;
  const failed = tasks.filter((task) => task.status === "failed" || task.status === "blocked").length;
  const completion = tasks.length ? Math.round(100 * successful / tasks.length) : 0;

  return (
    <main className="main overview">
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

      <BuildPanel state={state} openTimeline={() => setTimelineOpen(true)} />
      <TaskTable rows={rows} selected={selected} setSelected={setSelected} />
      {timelineOpen && <TimelineDrawer state={state} close={() => setTimelineOpen(false)} />}
      {selected && <ChapterInspector row={selected} close={() => setSelected(null)} />}
    </main>
  );
}

function KindIcon({ kind, size = 15 }: { kind: DeclarationKind; size?: number }) {
  if (kind === "theorem" || kind === "lemma") return <span className="kind-symbol" style={{ fontSize: size }}>⊢</span>;
  if (kind === "structure" || kind === "class") return <Box size={size} />;
  if (kind === "instance") return <Zap size={size} />;
  return <Code2 size={size} />;
}

function syntaxLine(line: string) {
  const commentAt = line.indexOf("--");
  const code = commentAt >= 0 ? line.slice(0, commentAt) : line;
  const comment = commentAt >= 0 ? line.slice(commentAt) : "";
  const parts = code.split(/(\b(?:theorem|lemma|def|abbrev|structure|class|instance|namespace|section|variable|noncomputable|where|by|fun|match|with|let|in|if|then|else|have|show|from|exact|rw|simpa|using|Type|Prop)\b|:=|→|↔|∀|∃|λ|⊤|⊥)/g);
  return <>{parts.map((part, index) => {
    if (/^(theorem|lemma|def|abbrev|structure|class|instance|namespace|section|variable|noncomputable|where|by|fun|match|with|let|in|if|then|else|have|show|from|exact|rw|simpa|using)$/.test(part)) return <span className="syn-keyword" key={index}>{part}</span>;
    if (/^(:=|→|↔|∀|∃|λ|⊤|⊥)$/.test(part)) return <span className="syn-operator" key={index}>{part}</span>;
    if (/^(Type|Prop)$/.test(part)) return <span className="syn-type" key={index}>{part}</span>;
    return <span key={index}>{part}</span>;
  })}{comment && <span className="syn-comment">{comment}</span>}</>;
}

function LeanCode({ statement }: { statement: LeanStatement }) {
  const lines = statement.excerpt.split("\n");
  return (
    <div className="code-view" role="region" aria-label={`${statement.name} source code`}>
      {lines.map((line, index) => (
        <div className="code-line" key={index}>
          <span className="line-number">{statement.line + index}</span>
          <code>{syntaxLine(line)}</code>
        </div>
      ))}
    </div>
  );
}

function Select({ value, onChange, children, label }: { value: string; onChange: (value: string) => void; children: React.ReactNode; label: string }) {
  return (
    <label className="select-wrap" title={label}>
      <select value={value} onChange={(event) => onChange(event.target.value)} aria-label={label}>{children}</select>
      <ChevronDown size={14} />
    </label>
  );
}

function StatementBrowser({ close, connected }: { close: () => void; connected: boolean }) {
  const [query, setQuery] = useState("");
  const [book, setBook] = useState("all");
  const [kind, setKind] = useState("all");
  const [status, setStatus] = useState("all");
  const [data, setData] = useState<StatementResponse>(demoStatementResponse);
  const [selected, setSelected] = useState<LeanStatement>(demoStatementResponse.declarations[0]);
  const [loading, setLoading] = useState(true);
  const [copied, setCopied] = useState(false);
  const searchRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
        event.preventDefault();
        searchRef.current?.focus();
      }
      if (event.key === "Escape" && document.activeElement === searchRef.current) {
        setQuery("");
        searchRef.current?.blur();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  useEffect(() => {
    let cancelled = false;
    const timer = window.setTimeout(async () => {
      setLoading(true);
      try {
        const params = new URLSearchParams({ q: query, book, kind, status, limit: "180" });
        const response = await fetch(`/api/statements?${params}`);
        if (!response.ok) throw new Error("statement index unavailable");
        const next = await response.json() as StatementResponse;
        if (cancelled) return;
        setData(next);
        setSelected((current) => next.declarations.find((item) => item.id === current?.id) ?? next.declarations[0] ?? current);
      } catch {
        if (!cancelled) {
          const filtered = demoStatementResponse.declarations.filter((declaration) =>
            (!query || `${declaration.name} ${declaration.signature} ${declaration.doc}`.toLowerCase().includes(query.toLowerCase())) &&
            (book === "all" || String(declaration.bookNumber) === book) &&
            (kind === "all" || declaration.kind === kind) &&
            (status === "all" || declaration.status === status),
          );
          setData({ ...demoStatementResponse, total: filtered.length, declarations: filtered });
          if (filtered[0]) setSelected(filtered[0]);
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    }, 180);
    return () => { cancelled = true; window.clearTimeout(timer); };
  }, [book, kind, query, status]);

  const copyStatement = async () => {
    if (!selected) return;
    await navigator.clipboard.writeText(selected.signature);
    setCopied(true);
    window.setTimeout(() => setCopied(false), 1400);
  };

  return (
    <main className="main statement-page">
      <div className="statement-heading">
        <button className="back-button" onClick={close}><ArrowLeft size={16} /> Overview</button>
        <div className="statement-title-block">
          <div><span className="eyebrow">Repository index</span><h1>Lean statement browser</h1></div>
          <div className="index-status"><Database size={14} /><span>{connected ? "filesystem index" : "demo index"}</span><strong>{formatNumber(data.facets.statuses.proved + data.facets.statuses.sorry)} declarations</strong></div>
        </div>
        <div className="statement-toolbar">
          <label className="statement-search">
            <Search size={18} />
            <input ref={searchRef} autoFocus value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Search names, types, docs…" />
            {query && <button onClick={() => setQuery("")} aria-label="Clear search"><X size={15} /></button>}
            <span className="key-hint">⌘ K</span>
          </label>
          <Select value={kind} onChange={setKind} label="Declaration kind">
            <option value="all">All kinds</option>
            {DECLARATION_KINDS.map((item) => <option value={item} key={item}>{item}</option>)}
          </Select>
          <Select value={status} onChange={setStatus} label="Proof status">
            <option value="all">Any proof status</option>
            <option value="proved">Proved</option>
            <option value="sorry">Contains sorry</option>
          </Select>
        </div>
      </div>

      <div className="browser-shell">
        <aside className="library-tree">
          <div className="browser-panel-title"><BookOpen size={15} /><span>Library</span></div>
          <button className={`tree-row all ${book === "all" ? "active" : ""}`} onClick={() => setBook("all")}>
            <span><HardDrive size={14} /> All books</span><em>{formatNumber(data.facets.statuses.proved + data.facets.statuses.sorry)}</em>
          </button>
          <div className="tree-label">Books</div>
          <div className="book-list">
            {data.facets.books.map((item) => (
              <button className={`tree-row ${book === item.id ? "active" : ""}`} key={item.id} onClick={() => setBook(item.id)} title={item.label}>
                <span><ChevronRight size={13} /> <b>{String(item.number).padStart(3, "0")}</b> {item.label}</span><em>{formatNumber(item.count)}</em>
              </button>
            ))}
          </div>
          <div className="tree-proof-summary">
            <div className="tree-label">Proof surface</div>
            <div><span><CheckCircle2 size={13} /> complete</span><strong>{formatNumber(data.facets.statuses.proved)}</strong></div>
            <div><span><CircleDashed size={13} /> sorry</span><strong>{formatNumber(data.facets.statuses.sorry)}</strong></div>
            <ProgressBar value={100 * data.facets.statuses.proved / Math.max(1, data.facets.statuses.proved + data.facets.statuses.sorry)} color="var(--green)" />
          </div>
        </aside>

        <section className="result-list-panel">
          <div className="browser-panel-title results-title">
            <span>{loading ? "Indexing…" : `${formatNumber(data.total)} matches`}</span>
            <span><Filter size={13} /> {book === "all" ? "all books" : `book ${String(book).padStart(3, "0")}`}</span>
          </div>
          <div className={`result-list ${loading ? "loading" : ""}`}>
            {data.declarations.map((statement) => (
              <button key={statement.id} className={`statement-result ${selected?.id === statement.id ? "active" : ""}`} onClick={() => setSelected(statement)}>
                <div className={`declaration-icon kind-${statement.kind}`}><KindIcon kind={statement.kind} /></div>
                <div className="statement-result-body">
                  <div><strong>{statement.name}</strong><span className={`proof-dot ${statement.status}`} title={statement.status} /></div>
                  <code>{statement.signature.split("\n").slice(1, 3).join(" ") || statement.signature}</code>
                  <span>Book {String(statement.bookNumber).padStart(3, "0")} · Ch {statement.chapter} · § {statement.section}</span>
                </div>
              </button>
            ))}
            {!data.declarations.length && <div className="no-results"><Search size={24} /><strong>No declarations found</strong><span>Try a broader name, kind, or proof filter.</span></div>}
          </div>
          {data.total > data.declarations.length && <div className="result-limit">First {data.declarations.length} shown · refine your search</div>}
        </section>

        <section className="statement-detail">
          {selected ? (
            <>
              <div className="detail-header">
                <div className="detail-kind"><KindIcon kind={selected.kind} size={17} /><span>{selected.kind}</span></div>
                <div className="detail-actions">
                  <button onClick={copyStatement}>{copied ? <Check size={14} /> : <Copy size={14} />}{copied ? "Copied" : "Copy statement"}</button>
                </div>
              </div>
              <div className="detail-title">
                <h2>{selected.name}</h2>
                <span className={`proof-status ${selected.status}`}>
                  {selected.status === "proved" ? <CheckCircle2 size={13} /> : <AlertTriangle size={13} />}
                  {selected.status === "proved" ? "proof complete" : "contains sorry"}
                </span>
              </div>
              {selected.doc && <p className="statement-doc">{selected.doc}</p>}
              <div className="source-path"><FileCode2 size={14} /><span>{selected.path}</span><strong>:{selected.line}</strong></div>
              <LeanCode statement={selected} />
              <div className="statement-meta">
                <div><span className="eyebrow">Location</span><strong>Book {String(selected.bookNumber).padStart(3, "0")} / Chapter {selected.chapter}</strong><p>{selected.book} · § {selected.section}</p></div>
                <div><span className="eyebrow">Declaration</span><strong>{selected.kind}</strong><p>lines {selected.line}–{selected.endLine}</p></div>
                <div><span className="eyebrow">Verification</span><strong className={selected.status === "proved" ? "success-text" : "warning-text"}>{selected.status === "proved" ? "closed term" : "open proof"}</strong><p>{selected.status === "proved" ? "no sorry in declaration" : "proof contains sorry"}</p></div>
              </div>
            </>
          ) : <div className="detail-empty"><Code2 size={28} /><span>Select a declaration to inspect its source.</span></div>}
        </section>
      </div>
    </main>
  );
}

export default function App() {
  const [view, setView] = useState<View>(window.location.hash === "#statements" ? "statements" : "overview");
  const [live, setLive] = useState(true);
  const {
    state,
    swarms,
    selectedSwarm,
    selectSwarm,
    connected,
    fetching,
    refresh,
  } = useSwarmState(live);

  const navigate = (next: View) => {
    setView(next);
    window.history.replaceState(null, "", next === "statements" ? "#statements" : window.location.pathname);
  };

  useEffect(() => {
    const openSearch = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k" && view !== "statements") {
        event.preventDefault();
        navigate("statements");
      }
    };
    window.addEventListener("keydown", openSearch);
    return () => window.removeEventListener("keydown", openSearch);
  }, [view]);

  return (
    <div className="app-shell">
      <Header
        view={view}
        setView={navigate}
        live={live}
        setLive={setLive}
        connected={connected}
        fetching={fetching}
        refresh={refresh}
        swarms={swarms}
        selectedSwarm={selectedSwarm}
        selectSwarm={selectSwarm}
      />
      <Rail view={view} setView={navigate} />
      {view === "overview"
        ? <Overview state={state} connected={connected} />
        : <StatementBrowser close={() => navigate("overview")} connected={connected} />}
      <footer className="status-bar">
        <span><span className={`footer-dot ${connected ? "connected" : ""}`} /> {connected ? state.source : "demo snapshot"}</span>
        <span><Clock3 size={12} /> state {timeAgo(state.updated_at)}</span>
        <span><GitBranch size={12} /> critical path: {state.scheduling?.statements?.critical_path?.join(" → ") || "—"}</span>
        <span className="status-spacer" />
        <span><HardDrive size={12} /> {state.isolation?.backend ?? "shared"}</span>
        <span><Command size={12} /> K search</span>
      </footer>
    </div>
  );
}
