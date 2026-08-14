import {
  Activity,
  Check,
  ChevronDown,
  Clock3,
  Code2,
  Command,
  FileCode2,
  GitBranch,
  HardDrive,
  Cpu,
  MemoryStick,
  LayoutDashboard,
  Pause,
  Play,
  RefreshCw,
  TerminalSquare,
  Users,
} from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { timeAgo } from "../lib/format";
import type { SwarmState, SwarmSummary, SystemLoad } from "../types";
import { IconButton } from "./Controls";

export type View = "overview" | "statements";

interface NavigationProps {
  view: View;
  setView: (view: View) => void;
}

interface HeaderProps extends NavigationProps {
  live: boolean;
  setLive: (live: boolean) => void;
  connected: boolean;
  fetching: boolean;
  refresh: () => Promise<void>;
  swarms: SwarmSummary[];
  selectedSwarm: string | null;
  selectSwarm: (swarmId: string) => void;
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
      <span className="swarm-item-agents"><strong>{swarm.active_agents}</strong><small>agents</small></span>
      {selected && <Check size={14} />}
    </button>
  );
}

export function SwarmSwitcher({
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
        <span className="swarm-trigger-copy"><small>Watching swarm</small><strong>{selected?.id ?? "demo-snapshot"}</strong></span>
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
          {active.map((swarm) => <SwarmMenuItem key={swarm.id} swarm={swarm} selected={swarm.id === selected?.id} onSelect={choose} />)}
          {recent.length > 0 && <div className="swarm-menu-label recent">Recent state</div>}
          {recent.map((swarm) => <SwarmMenuItem key={swarm.id} swarm={swarm} selected={swarm.id === selected?.id} onSelect={choose} />)}
          {!swarms.length && <div className="swarm-menu-empty">Repository API unavailable</div>}
        </div>
      )}
    </div>
  );
}

export function Header(props: HeaderProps) {
  const { view, setView, live, setLive, connected, fetching, refresh, swarms, selectedSwarm, selectSwarm } = props;
  return (
    <header className="app-header">
      <div className="brand" onClick={() => setView("overview")} role="button" tabIndex={0}>
        <div className="brand-glyph" aria-hidden="true"><span>λ</span></div>
        <div><div className="brand-name">LASTLIB</div><div className="brand-sub">FORMALIZATION OBSERVATORY</div></div>
      </div>
      <SwarmSwitcher swarms={swarms} selectedSwarm={selectedSwarm} onSelect={selectSwarm} />
      <nav className="top-nav" aria-label="Primary navigation">
        <button className={view === "overview" ? "active" : ""} onClick={() => setView("overview")}><LayoutDashboard size={14} /> Overview</button>
        <button className={view === "statements" ? "active" : ""} onClick={() => setView("statements")}><Code2 size={15} /> Statement browser</button>
      </nav>
      <div className="header-actions">
        <div className={`connection ${connected ? "connected" : "demo"}`} title={connected ? "Reading live repository state" : "Repository API unavailable; showing a demo snapshot"}>
          <span className="connection-dot" /><span>{connected ? "repository" : "demo mode"}</span>
        </div>
        <button className={`live-control ${live ? "on" : ""}`} onClick={() => setLive(!live)}>
          {live ? <Pause size={12} /> : <Play size={12} />}{live ? "Live" : "Paused"}
        </button>
        <IconButton label="Refresh now" onClick={() => void refresh()}><RefreshCw size={16} className={fetching ? "spinning" : ""} /></IconButton>
      </div>
    </header>
  );
}

export function Rail({ view, setView }: NavigationProps) {
  return (
    <aside className="rail">
      <div className="rail-main">
        <IconButton label="Overview" active={view === "overview"} onClick={() => setView("overview")}><LayoutDashboard size={19} /></IconButton>
        <IconButton label="Lean statements" active={view === "statements"} onClick={() => setView("statements")}><FileCode2 size={19} /></IconButton>
        <div className="rail-divider" />
        <IconButton label="Agents"><Users size={19} /></IconButton>
        <IconButton label="Build queue"><TerminalSquare size={19} /></IconButton>
        <IconButton label="Dependency graph"><GitBranch size={19} /></IconButton>
      </div>
      <div className="rail-foot"><span className="lean-version">L4</span></div>
    </aside>
  );
}

function gibibytes(bytes: number): string {
  return (bytes / 1024 ** 3).toFixed(1);
}

export function StatusBar({ state, connected, systemLoad }: { state: SwarmState; connected: boolean; systemLoad: SystemLoad | null }) {
  return (
    <footer className="status-bar">
      <span><span className={`footer-dot ${connected ? "connected" : ""}`} /> {connected ? state.source : "demo snapshot"}</span>
      <span><Clock3 size={12} /> state {timeAgo(state.updated_at)}</span>
      <span><GitBranch size={12} /> critical path: {state.scheduling?.statements?.critical_path?.join(" → ") || "—"}</span>
      <span className="status-spacer" />
      <span title="Host CPU utilization"><Cpu size={12} /> CPU {systemLoad?.cpu_percent == null ? "—" : `${systemLoad.cpu_percent.toFixed(0)}%`}</span>
      <span title={systemLoad ? `${systemLoad.memory_percent.toFixed(1)}% of host memory in use` : "Host memory unavailable"}>
        <MemoryStick size={12} /> RAM {systemLoad ? `${gibibytes(systemLoad.memory_used_bytes)} / ${gibibytes(systemLoad.memory_total_bytes)} GiB` : "—"}
      </span>
      <span><HardDrive size={12} /> {state.isolation?.backend ?? "shared"}</span>
      <span><Command size={12} /> K search</span>
    </footer>
  );
}
