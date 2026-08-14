import type { AgentActivity, Stage, SwarmState, Task } from "../../types";

export const STAGES: Stage[] = ["formalize", "fixup", "review", "prove"];

export interface ChapterRow {
  id: string;
  book: string;
  number: number;
  title: string;
  stages: Partial<Record<Stage, Task>>;
  activity?: AgentActivity;
  latestTask?: Task;
}

export function chapterRows(state: SwarmState): ChapterRow[] {
  const rows = new Map<string, ChapterRow>();
  Object.values(state.tasks ?? {}).forEach((task) => {
    const row = rows.get(task.chapter_id) ?? {
      id: task.chapter_id,
      book: task.book_id,
      number: task.chapter_number,
      title: task.chapter_title,
      stages: {},
    };
    row.stages[task.stage] = task;
    if (!row.latestTask || task.updated_at > row.latestTask.updated_at) row.latestTask = task;
    rows.set(task.chapter_id, row);
  });
  rows.forEach((row) => {
    const task = Object.values(row.stages)
      .filter((candidate): candidate is Task & { latest_run_id: string } => Boolean(
        candidate?.latest_run_id && state.activities?.[candidate.latest_run_id],
      ))
      .sort((left, right) => {
        const running = Number(right.status === "running") - Number(left.status === "running");
        if (running) return running;
        const leftUpdated = state.activities?.[left.latest_run_id]?.updated_at ?? left.updated_at;
        const rightUpdated = state.activities?.[right.latest_run_id]?.updated_at ?? right.updated_at;
        return rightUpdated.localeCompare(leftUpdated);
      })[0];
    if (task) row.activity = state.activities?.[task.latest_run_id];
  });
  const bookSortKey = (bookId: string): [number, number | string] => {
    const match = /^book(\d+)$/i.exec(bookId);
    return match ? [0, Number(match[1])] : [1, bookId.toLocaleLowerCase()];
  };
  return [...rows.values()].sort((left, right) => {
    const leftBook = bookSortKey(left.book);
    const rightBook = bookSortKey(right.book);
    if (leftBook[0] !== rightBook[0]) return leftBook[0] - rightBook[0];
    const bookOrder = typeof leftBook[1] === "number" && typeof rightBook[1] === "number"
      ? leftBook[1] - rightBook[1]
      : String(leftBook[1]).localeCompare(String(rightBook[1]));
    return bookOrder || left.number - right.number;
  });
}

export function chapterLabel(row: ChapterRow): string {
  const match = /^book(\d+)$/i.exec(row.book);
  return `${match ? Number(match[1]) : row.book}.${row.number}`;
}
