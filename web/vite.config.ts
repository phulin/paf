import react from "@vitejs/plugin-react";
import { createHash } from "node:crypto";
import { promises as fs } from "node:fs";
import path from "node:path";
import type { IncomingMessage, ServerResponse } from "node:http";
import { defineConfig, type Plugin } from "vite";

type DeclarationKind =
  | "theorem"
  | "lemma"
  | "def"
  | "abbrev"
  | "structure"
  | "class"
  | "instance";

interface IndexedDeclaration {
  id: string;
  name: string;
  kind: DeclarationKind;
  signature: string;
  excerpt: string;
  doc: string;
  path: string;
  line: number;
  endLine: number;
  book: string;
  bookNumber: number;
  chapter: number;
  section: string;
  status: "proved" | "sorry";
  search: string;
}

const webRoot = path.dirname(new URL(import.meta.url).pathname);
const repositoryRoot = path.resolve(webRoot, "..");
const leanRoot = path.join(repositoryRoot, "lean", "LastLib");
const swarmRoot = path.join(repositoryRoot, ".swarm");
const declarationPattern =
  /^\s*(?:(?:noncomputable|private|protected|unsafe|opaque)\s+)*(theorem|lemma|def|abbrev|structure|class|instance)\s+([^\s([{:=]+)/;

function words(value: string): string {
  return value
    .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
    .replace(/([A-Z])([A-Z][a-z])/g, "$1 $2")
    .replace(/_/g, " ")
    .trim();
}

async function walk(directory: string): Promise<string[]> {
  const entries = await fs.readdir(directory, { withFileTypes: true });
  const nested = await Promise.all(
    entries.map(async (entry) => {
      const target = path.join(directory, entry.name);
      if (entry.isDirectory()) return walk(target);
      return entry.isFile() && entry.name.endsWith(".lean") ? [target] : [];
    }),
  );
  return nested.flat();
}

function extractDoc(lines: string[], index: number): string {
  const collected: string[] = [];
  let cursor = index - 1;
  while (cursor >= 0 && !lines[cursor].includes("/--")) {
    if (lines[cursor].trim() && !lines[cursor].trim().startsWith("@[")) {
      collected.unshift(lines[cursor]);
    }
    if (index - cursor > 12) return "";
    cursor -= 1;
  }
  if (cursor < 0) return "";
  collected.unshift(lines[cursor]);
  return collected
    .join("\n")
    .replace(/^\s*\/--?/, "")
    .replace(/-\/\s*$/, "")
    .replace(/^\s*\*\s?/gm, "")
    .replace(/`([^`]+)`/g, "$1")
    .replace(/\s+/g, " ")
    .trim();
}

function extractSignature(lines: string[], start: number, stop: number): string {
  const header: string[] = [];
  for (let index = start; index < Math.min(stop, start + 30); index += 1) {
    const line = lines[index];
    const assignment = line.indexOf(":=");
    if (assignment >= 0) {
      header.push(`${line.slice(0, assignment).trimEnd()} := …`);
      break;
    }
    if (/\bwhere\s*$/.test(line)) {
      header.push(`${line.replace(/\bwhere\s*$/, "").trimEnd()} where …`);
      break;
    }
    header.push(line.trimEnd());
    if (/^\s*(structure|class)\b/.test(lines[start]) && /\bwhere\b/.test(line)) break;
  }
  return header.join("\n").trim();
}

function parseLeanFile(absolutePath: string, source: string): IndexedDeclaration[] {
  const relative = path.relative(repositoryRoot, absolutePath).split(path.sep).join("/");
  const pathParts = relative.split("/");
  const bookPart = pathParts.find((part) => /^Book\d+/.test(part)) ?? "Book00Unknown";
  const bookMatch = /^Book(\d+)(.*)$/.exec(bookPart);
  const chapterPart = pathParts.find((part) => /^Chapter\d+/.test(part)) ?? "Chapter00";
  const chapterMatch = /^Chapter(\d+)/.exec(chapterPart);
  const fileName = path.basename(absolutePath, ".lean");
  const sectionMatch = /^Section(\d+)(.*)$/.exec(fileName);
  const bookNumber = Number(bookMatch?.[1] ?? 0);
  const book = words(bookMatch?.[2] ?? "Unknown");
  const chapter = Number(chapterMatch?.[1] ?? 0);
  const section = sectionMatch
    ? `${Number(sectionMatch[1])}.${words(sectionMatch[2])}`
    : words(fileName);
  const lines = source.split("\n");
  const starts: Array<{ index: number; kind: DeclarationKind; name: string }> = [];

  lines.forEach((line, index) => {
    const match = declarationPattern.exec(line);
    if (match) {
      starts.push({ index, kind: match[1] as DeclarationKind, name: match[2] });
    }
  });

  return starts.map((start, position) => {
    const stop = starts[position + 1]?.index ?? lines.length;
    const excerptStart = Math.max(0, start.index - (extractDoc(lines, start.index) ? 1 : 0));
    const excerptEnd = Math.min(stop, start.index + 30);
    const excerpt = lines.slice(excerptStart, excerptEnd).join("\n").trimEnd();
    const signature = extractSignature(lines, start.index, stop);
    const doc = extractDoc(lines, start.index);
    const body = lines.slice(start.index, stop).join("\n");
    const id = createHash("sha1")
      .update(`${relative}:${start.index + 1}:${start.name}`)
      .digest("hex")
      .slice(0, 12);
    return {
      id,
      name: start.name,
      kind: start.kind,
      signature,
      excerpt,
      doc,
      path: relative,
      line: start.index + 1,
      endLine: excerptEnd,
      book,
      bookNumber,
      chapter,
      section,
      status: /\bsorry\b/.test(body) ? "sorry" : "proved",
      search: `${start.name} ${signature} ${doc} ${book} ${section}`.toLocaleLowerCase(),
    };
  });
}

let declarationCache: IndexedDeclaration[] | null = null;
let declarationCacheTime = 0;
const fileDeclarationCache = new Map<
  string,
  { modified: number; size: number; declarations: IndexedDeclaration[] }
>();

async function declarations(): Promise<IndexedDeclaration[]> {
  if (declarationCache && Date.now() - declarationCacheTime < 5_000) return declarationCache;
  const files = (await walk(leanRoot)).sort();
  const parsed = await Promise.all(
    files.map(async (file) => {
      const stat = await fs.stat(file);
      const cached = fileDeclarationCache.get(file);
      if (cached && cached.modified === stat.mtimeMs && cached.size === stat.size) {
        return cached.declarations;
      }
      const next = parseLeanFile(file, await fs.readFile(file, "utf8"));
      fileDeclarationCache.set(file, {
        modified: stat.mtimeMs,
        size: stat.size,
        declarations: next,
      });
      return next;
    }),
  );
  const currentFiles = new Set(files);
  for (const cachedFile of fileDeclarationCache.keys()) {
    if (!currentFiles.has(cachedFile)) fileDeclarationCache.delete(cachedFile);
  }
  declarationCache = parsed
    .flat()
    .sort((left, right) =>
      left.bookNumber - right.bookNumber ||
      left.chapter - right.chapter ||
      left.path.localeCompare(right.path) ||
      left.line - right.line,
    );
  declarationCacheTime = Date.now();
  return declarationCache;
}

interface SwarmCandidate {
  id: string;
  path: string;
  modified: number;
  state: Record<string, unknown>;
}

async function swarmCandidates(): Promise<SwarmCandidate[]> {
  let entries;
  try {
    entries = await fs.readdir(swarmRoot, { withFileTypes: true });
  } catch {
    return [];
  }
  const candidates = await Promise.all(
    entries
      .filter((entry) => entry.isDirectory())
      .map(async (entry) => {
        const statePath = path.join(swarmRoot, entry.name, "state.json");
        try {
          const [stat, contents] = await Promise.all([
            fs.stat(statePath),
            fs.readFile(statePath, "utf8"),
          ]);
          return {
            id: entry.name,
            path: statePath,
            modified: stat.mtimeMs,
            state: JSON.parse(contents) as Record<string, unknown>,
          };
        } catch {
          return null;
        }
      }),
  );
  return candidates
    .filter((candidate): candidate is NonNullable<typeof candidate> => Boolean(candidate))
    .sort((left, right) => right.modified - left.modified);
}

function swarmSummary(candidate: SwarmCandidate) {
  const tasks = (candidate.state.tasks ?? {}) as Record<
    string,
    { status?: string; book_id?: string }
  >;
  const agents = (candidate.state.agents ?? {}) as Record<string, unknown>;
  const build = (candidate.state.coordinator_build ?? {}) as Record<string, unknown>;
  const activeAgents = Number(agents.active ?? 0);
  return {
    id: candidate.id,
    active: activeAgents > 0 || build.active === true,
    updated_at: String(candidate.state.updated_at ?? new Date(candidate.modified).toISOString()),
    active_agents: activeAgents,
    maximum_agents: Number(agents.maximum ?? 0),
    queued_agents: Number(agents.queued ?? 0),
    running_tasks: Object.values(tasks).filter((task) => task.status === "running").length,
    task_count: Object.keys(tasks).length,
    book_count: new Set(Object.values(tasks).map((task) => task.book_id).filter(Boolean)).size,
  };
}

function json(response: ServerResponse, value: unknown, status = 200): void {
  response.statusCode = status;
  response.setHeader("Content-Type", "application/json; charset=utf-8");
  response.setHeader("Cache-Control", "no-store");
  response.end(JSON.stringify(value));
}

async function serveSwarms(response: ServerResponse): Promise<void> {
  const candidates = await swarmCandidates();
  json(response, {
    swarms: candidates
      .map(swarmSummary)
      .sort((left, right) => Number(right.active) - Number(left.active) || right.updated_at.localeCompare(left.updated_at)),
  });
}

async function serveSwarm(request: IncomingMessage, response: ServerResponse): Promise<void> {
  const candidates = await swarmCandidates();
  const url = new URL(request.url ?? "/api/swarm", "http://localhost");
  const requested = url.searchParams.get("swarm");
  const candidate = requested
    ? candidates.find((item) => item.id === requested)
    : candidates.find((item) => swarmSummary(item).active) ?? candidates[0];
  if (!candidate) {
    json(response, { error: "No .swarm state found" }, 404);
    return;
  }
  if (requested && candidate.id !== requested) {
    json(response, { error: `Unknown swarm: ${requested}` }, 404);
    return;
  }
  const state = candidate.state;
  const stateDir = path.dirname(candidate.path);
  const tasks = (state.tasks ?? {}) as Record<string, { latest_run_id?: string; updated_at?: string }>;
  const recentRuns = [...new Set(
    Object.values(tasks)
      .filter((task) => task.latest_run_id)
      .sort((left, right) => (right.updated_at ?? "").localeCompare(left.updated_at ?? ""))
      .slice(0, 36)
      .map((task) => task.latest_run_id as string),
  )];
  const activityPairs = await Promise.all(
    recentRuns.map(async (runId) => {
      try {
        const activity = JSON.parse(
          await fs.readFile(path.join(stateDir, "logs", `${runId}.activity.json`), "utf8"),
        );
        return [runId, activity] as const;
      } catch {
        return null;
      }
    }),
  );
  json(response, {
    ...state,
    swarm_id: candidate.id,
    source: path.relative(repositoryRoot, candidate.path),
    activities: Object.fromEntries(activityPairs.filter((pair) => pair !== null)),
  });
}

async function serveStatements(request: IncomingMessage, response: ServerResponse): Promise<void> {
  const url = new URL(request.url ?? "/api/statements", "http://localhost");
  const query = (url.searchParams.get("q") ?? "").trim().toLocaleLowerCase();
  const book = url.searchParams.get("book") ?? "all";
  const kind = url.searchParams.get("kind") ?? "all";
  const status = url.searchParams.get("status") ?? "all";
  const limit = Math.min(250, Math.max(1, Number(url.searchParams.get("limit") ?? 120)));
  const all = await declarations();
  const filtered = all.filter(
    (declaration) =>
      (!query || declaration.search.includes(query)) &&
      (book === "all" || String(declaration.bookNumber) === book) &&
      (kind === "all" || declaration.kind === kind) &&
      (status === "all" || declaration.status === status),
  );

  const bookCounts = new Map<number, { label: string; count: number }>();
  const kindCounts: Record<string, number> = {};
  let proved = 0;
  let sorry = 0;
  for (const declaration of all) {
    const current = bookCounts.get(declaration.bookNumber) ?? { label: declaration.book, count: 0 };
    current.count += 1;
    bookCounts.set(declaration.bookNumber, current);
    kindCounts[declaration.kind] = (kindCounts[declaration.kind] ?? 0) + 1;
    if (declaration.status === "proved") proved += 1;
    else sorry += 1;
  }

  json(response, {
    source: "repository",
    total: filtered.length,
    declarations: filtered.slice(0, limit).map(({ search: _, ...declaration }) => declaration),
    facets: {
      books: [...bookCounts.entries()]
        .sort(([left], [right]) => left - right)
        .map(([number, value]) => ({ id: String(number), number, ...value })),
      kinds: kindCounts,
      statuses: { proved, sorry },
    },
  });
}

function lastLibApi(): Plugin {
  const middleware = () => async (request: IncomingMessage, response: ServerResponse, next: () => void) => {
    try {
      if (request.url?.startsWith("/api/swarms")) {
        await serveSwarms(response);
        return;
      }
      if (request.url?.startsWith("/api/swarm")) {
        await serveSwarm(request, response);
        return;
      }
      if (request.url?.startsWith("/api/statements")) {
        await serveStatements(request, response);
        return;
      }
      next();
    } catch (error) {
      json(response, { error: error instanceof Error ? error.message : String(error) }, 500);
    }
  };
  return {
    name: "lastlib-repository-api",
    configureServer(server) {
      server.middlewares.use(middleware());
    },
    configurePreviewServer(server) {
      server.middlewares.use(middleware());
    },
  };
}

export default defineConfig({
  plugins: [react(), lastLibApi()],
  server: { port: 5173 },
  preview: { port: 4173 },
});
