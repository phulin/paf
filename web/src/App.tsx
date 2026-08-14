import { useEffect, useState } from "react";
import { Header, Rail, StatusBar, type View } from "./components/AppChrome";
import { Overview } from "./features/overview/Overview";
import { StatementBrowser } from "./features/statements/StatementBrowser";
import { useSwarmState } from "./hooks/useSwarmState";

export default function App() {
  const [view, setView] = useState<View>(window.location.hash === "#statements" ? "statements" : "overview");
  const [live, setLive] = useState(true);
  const { state, swarms, selectedSwarm, selectSwarm, connected, fetching, systemLoad, refresh } = useSwarmState(live);

  const navigate = (next: View) => {
    setView(next);
    const url = new URL(window.location.href);
    url.hash = next === "statements" ? "statements" : "";
    window.history.replaceState(window.history.state, "", `${url.pathname}${url.search}${url.hash}`);
  };

  useEffect(() => {
    const restoreView = () => setView(window.location.hash === "#statements" ? "statements" : "overview");
    window.addEventListener("popstate", restoreView);
    return () => window.removeEventListener("popstate", restoreView);
  }, []);

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
      <StatusBar state={state} connected={connected} systemLoad={systemLoad} />
    </div>
  );
}
