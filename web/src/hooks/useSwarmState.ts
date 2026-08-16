import { useCallback, useEffect, useRef, useState } from "react";
import { demoState } from "../demo";
import { corpusFromUrl, setCorpusUrl } from "../lib/urlState";
import type { DashboardDelta, SwarmState, SwarmSummary, SystemLoad } from "../types";

const MAX_RECENT_ACTIVITIES = 36;

export function applyDashboardDelta(previous: SwarmState, delta: DashboardDelta): SwarmState {
  const tasks = { ...previous.tasks, ...delta.tasks };
  delta.removed_task_ids.forEach((taskId) => delete tasks[taskId]);
  const activities = { ...(previous.activities ?? {}), ...delta.activities };
  const retainedRunIds = new Set([
    ...delta.active_run_ids,
    ...Object.values(tasks)
      .filter((task) => task.latest_run_id)
      .sort((left, right) => right.updated_at.localeCompare(left.updated_at))
      .slice(0, MAX_RECENT_ACTIVITIES)
      .map((task) => task.latest_run_id as string),
  ]);
  const retainedActivities = Object.fromEntries(
    Object.entries(activities).filter(([runId]) => retainedRunIds.has(runId)),
  );
  return {
    ...previous,
    ...delta.globals,
    revision: delta.revision,
    tasks,
    activities: retainedActivities,
  };
}

export function useSwarmState(live: boolean) {
  const [state, setState] = useState<SwarmState>(demoState);
  const [swarms, setSwarms] = useState<SwarmSummary[]>([]);
  const [selectedSwarm, setSelectedSwarm] = useState<string | null>(
    () => corpusFromUrl() ?? window.localStorage.getItem("paf.selectedSwarm"),
  );
  const [connected, setConnected] = useState(false);
  const [fetching, setFetching] = useState(true);
  const [systemLoad, setSystemLoad] = useState<SystemLoad | null>(null);
  const loadedRevision = useRef<Record<string, number>>({});
  const displayedSwarm = useRef<string | null>(null);
  const refreshSequence = useRef(0);

  const refresh = useCallback(async () => {
    const sequence = ++refreshSequence.current;
    setFetching(true);
    try {
      const [listResponse, systemResponse] = await Promise.all([
        fetch("/api/swarms", { cache: "no-store" }),
        fetch("/api/system", { cache: "no-store" }),
      ]);
      if (!listResponse.ok) throw new Error(`swarm list endpoint returned ${listResponse.status}`);
      if (systemResponse.ok) setSystemLoad((await systemResponse.json()) as SystemLoad);
      const list = ((await listResponse.json()) as { swarms: SwarmSummary[] }).swarms;
      setSwarms(list);
      const target = list.some((swarm) => swarm.id === selectedSwarm)
        ? selectedSwarm
        : (list.find((swarm) => swarm.active)?.id ?? list[0]?.id ?? null);
      if (target !== selectedSwarm) setSelectedSwarm(target);
      if (target) {
        window.localStorage.setItem("paf.selectedSwarm", target);
        if (corpusFromUrl() !== target) setCorpusUrl(target, "replace");
      }
      if (target) {
        const targetSummary = list.find((swarm) => swarm.id === target);
        const params = `?swarm=${encodeURIComponent(target)}`;
        const loadSnapshot = async () => {
          const response = await fetch(`/api/swarm${params}`, { cache: "no-store" });
          if (!response.ok) throw new Error(`state endpoint returned ${response.status}`);
          const snapshot = (await response.json()) as SwarmState;
          if (sequence !== refreshSequence.current) return;
          setState(snapshot);
          displayedSwarm.current = target;
          loadedRevision.current[target] = snapshot.revision ?? 0;
        };
        const revision = loadedRevision.current[target];
        if (displayedSwarm.current !== target || revision === undefined) {
          await loadSnapshot();
        } else if (targetSummary?.active || targetSummary?.revision !== revision) {
          const deltaParams = new URLSearchParams({
            swarm: target,
            after: String(revision),
            view: "dashboard",
          });
          const response = await fetch(`/api/changes?${deltaParams}`, { cache: "no-store" });
          if (!response.ok) throw new Error(`change endpoint returned ${response.status}`);
          const delta = (await response.json()) as DashboardDelta;
          if (sequence !== refreshSequence.current) return;
          if (delta.resync_required) {
            await loadSnapshot();
          } else {
            setState((previous) => applyDashboardDelta(previous, delta));
            loadedRevision.current[target] = delta.revision;
          }
        }
      }
      if (sequence !== refreshSequence.current) return;
      setConnected(true);
    } catch {
      if (sequence === refreshSequence.current) setConnected(false);
    } finally {
      if (sequence === refreshSequence.current) setFetching(false);
    }
  }, [selectedSwarm]);

  const selectSwarm = useCallback(
    (swarmId: string) => {
      if (swarmId === selectedSwarm) return;
      window.localStorage.setItem("paf.selectedSwarm", swarmId);
      setCorpusUrl(swarmId, "push");
      setSelectedSwarm(swarmId);
    },
    [selectedSwarm],
  );

  useEffect(() => {
    const restoreCorpus = () => {
      const corpus = corpusFromUrl();
      if (corpus) window.localStorage.setItem("paf.selectedSwarm", corpus);
      setSelectedSwarm(corpus);
    };
    window.addEventListener("popstate", restoreCorpus);
    return () => window.removeEventListener("popstate", restoreCorpus);
  }, []);

  useEffect(() => {
    void refresh();
    if (!live) return;
    const timer = window.setInterval(() => void refresh(), 3000);
    return () => window.clearInterval(timer);
  }, [live, refresh]);

  return { state, swarms, selectedSwarm, selectSwarm, connected, fetching, systemLoad, refresh };
}
