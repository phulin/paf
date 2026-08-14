import {
  AlertTriangle,
  ArrowLeft,
  BookOpen,
  Check,
  CheckCircle2,
  ChevronRight,
  CircleDashed,
  Code2,
  Copy,
  Database,
  FileCode2,
  Filter,
  HardDrive,
  Search,
  X,
} from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { ProgressBar, Select } from "../../components/Controls";
import { demoStatementResponse } from "../../demo";
import { formatNumber } from "../../lib/format";
import type { DeclarationKind, LeanStatement, StatementResponse } from "../../types";
import { KindIcon, LeanCode } from "./LeanCode";

const DECLARATION_KINDS: DeclarationKind[] = ["theorem", "lemma", "def", "abbrev", "structure", "class", "instance"];

export function StatementBrowser({ close, connected }: { close: () => void; connected: boolean }) {
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
          <label className="statement-search"><Search size={18} /><input ref={searchRef} autoFocus value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Search names, types, docs…" />{query && <button onClick={() => setQuery("")} aria-label="Clear search"><X size={15} /></button>}<span className="key-hint">⌘ K</span></label>
          <Select value={kind} onChange={setKind} label="Declaration kind"><option value="all">All kinds</option>{DECLARATION_KINDS.map((item) => <option value={item} key={item}>{item}</option>)}</Select>
          <Select value={status} onChange={setStatus} label="Proof status"><option value="all">Any proof status</option><option value="proved">Proved</option><option value="sorry">Contains sorry</option></Select>
        </div>
      </div>

      <div className="browser-shell">
        <LibraryTree data={data} book={book} setBook={setBook} />
        <ResultList data={data} book={book} loading={loading} selected={selected} setSelected={setSelected} />
        <StatementDetail selected={selected} copied={copied} copyStatement={copyStatement} />
      </div>
    </main>
  );
}

function LibraryTree({ data, book, setBook }: { data: StatementResponse; book: string; setBook: (book: string) => void }) {
  const total = data.facets.statuses.proved + data.facets.statuses.sorry;
  return (
    <aside className="library-tree">
      <div className="browser-panel-title"><BookOpen size={15} /><span>Library</span></div>
      <button className={`tree-row all ${book === "all" ? "active" : ""}`} onClick={() => setBook("all")}><span><HardDrive size={14} /> All books</span><em>{formatNumber(total)}</em></button>
      <div className="tree-label">Books</div>
      <div className="book-list">{data.facets.books.map((item) => <button className={`tree-row ${book === item.id ? "active" : ""}`} key={item.id} onClick={() => setBook(item.id)} title={item.label}><span><ChevronRight size={13} /> <b>{String(item.number).padStart(3, "0")}</b> {item.label}</span><em>{formatNumber(item.count)}</em></button>)}</div>
      <div className="tree-proof-summary">
        <div className="tree-label">Proof surface</div>
        <div><span><CheckCircle2 size={13} /> complete</span><strong>{formatNumber(data.facets.statuses.proved)}</strong></div>
        <div><span><CircleDashed size={13} /> sorry</span><strong>{formatNumber(data.facets.statuses.sorry)}</strong></div>
        <ProgressBar value={100 * data.facets.statuses.proved / Math.max(1, total)} color="var(--green)" />
      </div>
    </aside>
  );
}

function ResultList({ data, book, loading, selected, setSelected }: { data: StatementResponse; book: string; loading: boolean; selected: LeanStatement; setSelected: (item: LeanStatement) => void }) {
  return (
    <section className="result-list-panel">
      <div className="browser-panel-title results-title"><span>{loading ? "Indexing…" : `${formatNumber(data.total)} matches`}</span><span><Filter size={13} /> {book === "all" ? "all books" : `book ${String(book).padStart(3, "0")}`}</span></div>
      <div className={`result-list ${loading ? "loading" : ""}`}>
        {data.declarations.map((statement) => (
          <button key={statement.id} className={`statement-result ${selected?.id === statement.id ? "active" : ""}`} onClick={() => setSelected(statement)}>
            <div className={`declaration-icon kind-${statement.kind}`}><KindIcon kind={statement.kind} /></div>
            <div className="statement-result-body"><div><strong>{statement.name}</strong><span className={`proof-dot ${statement.status}`} title={statement.status} /></div><code>{statement.signature.split("\n").slice(1, 3).join(" ") || statement.signature}</code><span>Book {String(statement.bookNumber).padStart(3, "0")} · Ch {statement.chapter} · § {statement.section}</span></div>
          </button>
        ))}
        {!data.declarations.length && <div className="no-results"><Search size={24} /><strong>No declarations found</strong><span>Try a broader name, kind, or proof filter.</span></div>}
      </div>
      {data.total > data.declarations.length && <div className="result-limit">First {data.declarations.length} shown · refine your search</div>}
    </section>
  );
}

function StatementDetail({ selected, copied, copyStatement }: { selected?: LeanStatement; copied: boolean; copyStatement: () => void }) {
  return (
    <section className="statement-detail">
      {selected ? <>
        <div className="detail-header"><div className="detail-kind"><KindIcon kind={selected.kind} size={17} /><span>{selected.kind}</span></div><div className="detail-actions"><button onClick={copyStatement}>{copied ? <Check size={14} /> : <Copy size={14} />}{copied ? "Copied" : "Copy statement"}</button></div></div>
        <div className="detail-title"><h2>{selected.name}</h2><span className={`proof-status ${selected.status}`}>{selected.status === "proved" ? <CheckCircle2 size={13} /> : <AlertTriangle size={13} />}{selected.status === "proved" ? "proof complete" : "contains sorry"}</span></div>
        {selected.doc && <p className="statement-doc">{selected.doc}</p>}
        <div className="source-path"><FileCode2 size={14} /><span>{selected.path}</span><strong>:{selected.line}</strong></div>
        <LeanCode statement={selected} />
        <div className="statement-meta">
          <div><span className="eyebrow">Location</span><strong>Book {String(selected.bookNumber).padStart(3, "0")} / Chapter {selected.chapter}</strong><p>{selected.book} · § {selected.section}</p></div>
          <div><span className="eyebrow">Declaration</span><strong>{selected.kind}</strong><p>lines {selected.line}–{selected.endLine}</p></div>
          <div><span className="eyebrow">Verification</span><strong className={selected.status === "proved" ? "success-text" : "warning-text"}>{selected.status === "proved" ? "closed term" : "open proof"}</strong><p>{selected.status === "proved" ? "no sorry in declaration" : "proof contains sorry"}</p></div>
        </div>
      </> : <div className="detail-empty"><Code2 size={28} /><span>Select a declaration to inspect its source.</span></div>}
    </section>
  );
}
