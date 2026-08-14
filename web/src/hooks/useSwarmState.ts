import { useCallback, useEffect, useState } from "react";
import { demoState } from "../demo";
import { corpusFromUrl, setCorpusUrl } from "../lib/urlState";
import type { SwarmState, SwarmSummary } from "../types";

export function useSwarmState(live: boolean) {
  const [state, setState] = useState<SwarmState>(demoState);
  const [swarms, setSwarms] = useState<SwarmSummary[]>([]);
  const [selectedSwarm, setSelectedSwarm] = useState<string | null>(() =>
    corpusFromUrl() ?? window.localStorage.getItem("lastlib.selectedSwarm"),
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
      if (target) {
        window.localStorage.setItem("lastlib.selectedSwarm", target);
        if (corpusFromUrl() !== target) setCorpusUrl(target, "replace");
      }
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
    if (swarmId === selectedSwarm) return;
    window.localStorage.setItem("lastlib.selectedSwarm", swarmId);
    setCorpusUrl(swarmId, "push");
    setSelectedSwarm(swarmId);
  }, [selectedSwarm]);

  useEffect(() => {
    const restoreCorpus = () => {
      const corpus = corpusFromUrl();
      if (corpus) window.localStorage.setItem("lastlib.selectedSwarm", corpus);
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

  return { state, swarms, selectedSwarm, selectSwarm, connected, fetching, refresh };
}
