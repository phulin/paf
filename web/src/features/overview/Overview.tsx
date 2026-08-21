import { useEffect, useMemo, useState } from "react";
import { chapterFromUrl, setChapterUrl } from "../../lib/urlState";
import type { SwarmState } from "../../types";
import { ChapterInspector } from "./ChapterInspector";
import { DashboardSummary } from "./DashboardSummary";
import { PackageInspector } from "./PackageInspector";
import { BuildPanel, TimelineDrawer } from "./Timeline";
import { TaskTable } from "./TaskTable";
import { chapterRows, type ChapterRow } from "./model";

export function Overview({ state, connected }: { state: SwarmState; connected: boolean }) {
  const rows = useMemo(() => chapterRows(state), [state]);
  const [selectedChapter, setSelectedChapter] = useState<string | null>(() => chapterFromUrl());
  const [timelineOpen, setTimelineOpen] = useState(false);
  const [packagesOpen, setPackagesOpen] = useState(false);
  const selected = rows.find((row) => row.id === selectedChapter) ?? null;

  const inspectChapter = (row: ChapterRow | null) => {
    if (!row || row.id === selectedChapter) return;
    setChapterUrl(row.id, "push");
    setSelectedChapter(row.id);
  };

  const closeChapter = () => {
    setSelectedChapter(null);
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
    const openPackages = (event: KeyboardEvent) => {
      if (event.key.toLowerCase() !== "k" || event.metaKey || event.ctrlKey || event.altKey) return;
      const target = event.target as HTMLElement | null;
      if (target?.matches("input, textarea, select, [contenteditable='true']")) return;
      setPackagesOpen(true);
    };
    window.addEventListener("keydown", openPackages);
    return () => window.removeEventListener("keydown", openPackages);
  }, []);

  return (
    <main className="main overview">
      <DashboardSummary
        state={state}
        rows={rows}
        connected={connected}
        openPackages={() => setPackagesOpen(true)}
      />
      <BuildPanel state={state} openTimeline={() => setTimelineOpen(true)} />
      <TaskTable
        rows={rows}
        build={state.coordinator_build}
        selected={selected}
        setSelected={inspectChapter}
      />
      {timelineOpen && <TimelineDrawer state={state} close={() => setTimelineOpen(false)} />}
      {packagesOpen && <PackageInspector state={state} close={() => setPackagesOpen(false)} />}
      {selected && !packagesOpen && (
        <ChapterInspector row={selected} close={closeChapter} activity={selected.activity} />
      )}
    </main>
  );
}
