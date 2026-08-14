import { ChevronRight, ListFilter, Search } from "lucide-react";
import { useState } from "react";
import { ActivityCell, StatusPill } from "./TaskStatus";
import { chapterLabel, STAGES, type ChapterRow } from "./model";

export function TaskTable({
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
        <div><span className="eyebrow">Chapter matrix</span><h2>Formalization queue</h2></div>
        <div className="table-tools">
          <div className="mini-tabs">
            {(["active", "all", "issues"] as const).map((option) => (
              <button key={option} className={filter === option ? "active" : ""} onClick={() => setFilter(option)}>{option}</button>
            ))}
          </div>
          <label className="compact-search"><Search size={14} /><input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="filter chapters" /></label>
        </div>
      </div>
      <div className="table-scroll">
        <table className="task-table">
          <thead><tr><th>Book / chapter</th>{STAGES.map((stage) => <th key={stage}>{stage}</th>)}<th>Current agent activity</th><th aria-label="Inspect" /></tr></thead>
          <tbody>
            {visible.slice(0, 22).map((row) => (
              <tr key={row.id} className={selected?.id === row.id ? "selected" : ""} onClick={() => setSelected(row)}>
                <td><div className="chapter-cell"><span className="chapter-index">{chapterLabel(row)}</span><div><strong>{row.title}</strong></div></div></td>
                {STAGES.map((stage) => <td key={stage}><StatusPill status={row.stages[stage]?.status} rounds={row.stages[stage]?.rounds} /></td>)}
                <td><ActivityCell activity={row.activity} task={row.latestTask} /></td>
                <td><ChevronRight className="row-chevron" size={16} /></td>
              </tr>
            ))}
          </tbody>
        </table>
        {!visible.length && <div className="empty-table"><ListFilter size={22} /><span>No chapters match this view.</span></div>}
      </div>
      <div className="table-footer"><span>Showing {Math.min(visible.length, 22)} of {visible.length} chapters</span><span>Click a row to inspect agent state</span></div>
    </section>
  );
}
