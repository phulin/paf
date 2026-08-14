const CORPUS_QUERY_PARAM = "corpus";
const CHAPTER_QUERY_PARAM = "chapter";

type HistoryMode = "push" | "replace";

function updateHistory(url: URL, state: Record<string, unknown>, mode: HistoryMode): void {
  const next = `${url.pathname}${url.search}${url.hash}`;
  if (mode === "push") window.history.pushState(state, "", next);
  else window.history.replaceState(state, "", next);
}

export function corpusFromUrl(): string | null {
  return new URL(window.location.href).searchParams.get(CORPUS_QUERY_PARAM);
}

export function setCorpusUrl(corpus: string, mode: HistoryMode): void {
  const url = new URL(window.location.href);
  url.searchParams.set(CORPUS_QUERY_PARAM, corpus);
  const historyState = { ...(window.history.state ?? {}), corpus };
  delete historyState.lastlibChapterView;
  updateHistory(url, historyState, mode);
}

export function chapterFromUrl(): string | null {
  return new URL(window.location.href).searchParams.get(CHAPTER_QUERY_PARAM);
}

export function setChapterUrl(chapter: string | null, mode: HistoryMode): void {
  const url = new URL(window.location.href);
  if (chapter) url.searchParams.set(CHAPTER_QUERY_PARAM, chapter);
  else url.searchParams.delete(CHAPTER_QUERY_PARAM);
  const historyState = { ...(window.history.state ?? {}) };
  if (chapter) {
    historyState.chapter = chapter;
    historyState.lastlibChapterView = mode === "push";
  } else {
    delete historyState.chapter;
    delete historyState.lastlibChapterView;
  }
  updateHistory(url, historyState, mode);
}
