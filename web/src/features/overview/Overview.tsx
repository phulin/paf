import { useEffect, useMemo, useState } from "react";
import { chapterFromUrl, setChapterUrl } from "../../lib/urlState";
import type { SwarmState } from "../../types";
import { ChapterInspector } from "./ChapterInspector";
import { DashboardSummary } from "./DashboardSummary";
import { BuildPanel, TimelineDrawer } from "./Timeline";
import { TaskTable } from "./TaskTable";
import { chapterRows, type ChapterRow } from "./model";

export function Overview({ state, connected }: { state: SwarmState; connected: boolean }) {
  const rows = useMemo(() => chapterRows(state), [state]);
  const [selectedChapter, setSelectedChapter] = useState<string | null>(() => chapterFromUrl());
  const [timelineOpen, setTimelineOpen] = useState(false);
  const selected = rows.find((row) => row.id === selectedChapter) ?? null;

  const inspectChapter = (row: ChapterRow | null) => {
    if (!row || row.id === selectedChapter) return;
    setChapterUrl(row.id, "push");
    setSelectedChapter(row.id);
  };

  const closeChapter = () => {
    setSelectedChapter(null);
    if (chapterFromUrl() && window.history.state?.lastlibChapterView === true) window.history.back();
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

  return (
    <main className="main overview">
      <DashboardSummary state={state} rows={rows} connected={connected} />
      <BuildPanel state={state} openTimeline={() => setTimelineOpen(true)} />
      <TaskTable rows={rows} selected={selected} setSelected={inspectChapter} />
      {timelineOpen && <TimelineDrawer state={state} close={() => setTimelineOpen(false)} />}
      {selected && <ChapterInspector row={selected} close={closeChapter} />}
    </main>
  );
}
