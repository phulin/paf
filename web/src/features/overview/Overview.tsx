import { useEffect, useMemo, useState } from "react";
import { chapterFromUrl, setChapterUrl } from "../../lib/urlState";
import type { ShepherdAgent, SwarmState } from "../../types";
import { ChapterInspector } from "./ChapterInspector";
import { DashboardSummary } from "./DashboardSummary";
import { ShepherdInspector } from "./ShepherdInspector";
import { BuildPanel, TimelineDrawer } from "./Timeline";
import { TaskTable } from "./TaskTable";
import { chapterRows, type ChapterRow } from "./model";

export function Overview({ state, connected }: { state: SwarmState; connected: boolean }) {
  const rows = useMemo(() => chapterRows(state), [state]);
  const [selectedChapter, setSelectedChapter] = useState<string | null>(() => chapterFromUrl());
  const [timelineOpen, setTimelineOpen] = useState(false);
  const [shepherdOpen, setShepherdOpen] = useState(false);
  const [selectedAgent, setSelectedAgent] = useState<ShepherdAgent | null>(null);
  const selected = rows.find((row) => row.id === selectedChapter) ?? null;

  const inspectChapter = (row: ChapterRow | null) => {
    if (!row || row.id === selectedChapter) return;
    setChapterUrl(row.id, "push");
    setSelectedChapter(row.id);
    setSelectedAgent(null);
  };

  const closeChapter = () => {
    setSelectedChapter(null);
    setSelectedAgent(null);
    if (chapterFromUrl() && window.history.state?.pafChapterView === true) window.history.back();
    else setChapterUrl(null, "replace");
  };

  useEffect(() => {
    const restoreChapter = () => setSelectedChapter(chapterFromUrl());
    window.addEventListener("popstate", restoreChapter);
    return () => window.removeEventListener("popstate", restoreChapter);
  }, []);

  useEffect(() => {
    if (!connected || !selectedChapter || rows.some((row) => row.id === selectedChapter)) return;
    setSelectedChapter(null);
    setChapterUrl(null, "replace");
  }, [connected, rows, selectedChapter]);

  useEffect(() => {
    const openShepherd = (event: KeyboardEvent) => {
      if (event.key.toLowerCase() !== "s" || event.metaKey || event.ctrlKey || event.altKey) return;
      const target = event.target as HTMLElement | null;
      if (target?.matches("input, textarea, select, [contenteditable='true']")) return;
      setShepherdOpen(true);
    };
    window.addEventListener("keydown", openShepherd);
    return () => window.removeEventListener("keydown", openShepherd);
  }, []);

  const openAgent = (agent: ShepherdAgent) => {
    const row = rows.find((candidate) => candidate.id === agent.work_unit_id);
    if (!row) return;
    setChapterUrl(row.id, "push");
    setSelectedChapter(row.id);
    setSelectedAgent(agent);
    setShepherdOpen(false);
  };

  return (
    <main className="main overview">
      <DashboardSummary
        state={state}
        rows={rows}
        connected={connected}
        openShepherd={() => setShepherdOpen(true)}
      />
      <BuildPanel state={state} openTimeline={() => setTimelineOpen(true)} />
      <TaskTable
        rows={rows}
        build={state.coordinator_build}
        selected={selected}
        setSelected={inspectChapter}
      />
      {timelineOpen && <TimelineDrawer state={state} close={() => setTimelineOpen(false)} />}
      {shepherdOpen && (
        <ShepherdInspector
          state={state}
          close={() => setShepherdOpen(false)}
          openAgent={openAgent}
        />
      )}
      {selected && !shepherdOpen && (
        <ChapterInspector
          row={selected}
          close={closeChapter}
          activity={
            selectedAgent?.run_id ? state.activities?.[selectedAgent.run_id] : selected.activity
          }
          runLabel={
            selectedAgent
              ? `${selectedAgent.label} · ${selectedAgent.stage} · ${selectedAgent.run_id.slice(0, 12)}`
              : undefined
          }
        />
      )}
    </main>
  );
}
