import { Activity, AlertTriangle, BookOpen, Check, CheckCircle2, ChevronDown, FileCode2, GitBranch } from "lucide-react";
import type { AgentActivity } from "../../types";

interface StructuredAgentReport {
  changed: boolean;
  complete: boolean;
  summary: string;
  issues: string[];
  fixupFindings: Array<{ description: string; ownerPaths: string[] }>;
  sourceIssues: Array<{ location: string; sourceExcerpt: string; description: string; suggestedCorrection: string }>;
}

export function parseAgentReport(value?: string): StructuredAgentReport | null {
  if (!value) return null;
  try {
    const report = JSON.parse(value) as Record<string, unknown>;
    if (typeof report.summary !== "string") return null;
    const strings = (item: unknown) => Array.isArray(item) ? item.filter((entry): entry is string => typeof entry === "string") : [];
    const fixupFindings = Array.isArray(report.fixup_findings)
      ? report.fixup_findings.flatMap((item) => {
          if (!item || typeof item !== "object") return [];
          const finding = item as Record<string, unknown>;
          if (typeof finding.description !== "string") return [];
          return [{ description: finding.description, ownerPaths: strings(finding.owner_paths) }];
        })
      : [];
    const sourceIssues = Array.isArray(report.source_issues)
      ? report.source_issues.flatMap((item) => {
          if (!item || typeof item !== "object") return [];
          const issue = item as Record<string, unknown>;
          if (typeof issue.description !== "string") return [];
          return [{
            location: typeof issue.location === "string" ? issue.location : "Source location not supplied",
            sourceExcerpt: typeof issue.source_excerpt === "string" ? issue.source_excerpt : "",
            description: issue.description,
            suggestedCorrection: typeof issue.suggested_correction === "string" ? issue.suggested_correction : "",
          }];
        })
      : [];
    return {
      changed: report.changed === true,
      complete: report.complete === true,
      summary: report.summary,
      issues: strings(report.issues),
      fixupFindings,
      sourceIssues,
    };
  } catch {
    return null;
  }
}

export function AgentUpdate({ activity }: { activity?: AgentActivity }) {
  const value = activity?.latest_summary;
  const report = parseAgentReport(value);
  if (!report) return <div className="agent-report plain"><p className="agent-summary">{value || activity?.current || "No agent update recorded for this chapter."}</p></div>;
  const findingCount = report.issues.length + report.fixupFindings.length + report.sourceIssues.length;
  return (
    <div className="agent-report">
      <div className="agent-report-status">
        <span className={`report-badge ${report.complete ? "complete" : "working"}`}>{report.complete ? <CheckCircle2 size={12} /> : <Activity size={12} />}{report.complete ? "complete" : "in progress"}</span>
        <span className={`report-badge ${report.changed ? "changed" : "unchanged"}`}>{report.changed ? <FileCode2 size={12} /> : <Check size={12} />}{report.changed ? "files changed" : "no changes"}</span>
      </div>
      <p className="agent-summary">{report.summary}</p>
      {findingCount === 0 && <div className="report-clear"><CheckCircle2 size={13} /><span>No issues or follow-up findings reported</span></div>}
      {report.issues.length > 0 && (
        <div className="report-group issues"><div className="report-group-title"><AlertTriangle size={13} /><span>Issues</span><em>{report.issues.length}</em></div><ul>{report.issues.map((issue, index) => <li key={index}>{issue}</li>)}</ul></div>
      )}
      {report.fixupFindings.length > 0 && (
        <div className="report-group fixups">
          <div className="report-group-title"><GitBranch size={13} /><span>Fixup findings</span><em>{report.fixupFindings.length}</em></div>
          {report.fixupFindings.map((finding, index) => <div className="report-finding" key={index}><p>{finding.description}</p>{finding.ownerPaths.length > 0 && <div className="owner-paths">{finding.ownerPaths.map((path) => <code key={path}>{path}</code>)}</div>}</div>)}
        </div>
      )}
      {report.sourceIssues.length > 0 && (
        <div className="report-group source-issues">
          <div className="report-group-title"><BookOpen size={13} /><span>Source issues</span><em>{report.sourceIssues.length}</em></div>
          {report.sourceIssues.map((issue, index) => (
            <details className="source-issue" key={index}>
              <summary><span>{issue.location}</span><ChevronDown size={13} /></summary><p>{issue.description}</p>
              {issue.sourceExcerpt && <blockquote>{issue.sourceExcerpt}</blockquote>}
              {issue.suggestedCorrection && <div className="suggested-fix"><strong>Suggested correction</strong><p>{issue.suggestedCorrection}</p></div>}
            </details>
          ))}
        </div>
      )}
    </div>
  );
}

export function AgentPlan({ activity }: { activity?: AgentActivity }) {
  const todos = activity?.todos ?? [];
  const completed = todos.filter((todo) => todo.completed).length;
  const total = activity?.todo_total ?? todos.length;
  const progress = total > 0 ? Math.min(100, Math.round(((activity?.todo_completed ?? completed) / total) * 100)) : 0;
  const currentIndex = todos.findIndex((todo) => !todo.completed);
  return (
    <div className="agent-plan">
      <div className="agent-plan-heading"><span className="eyebrow">Current agent plan</span>{total > 0 && <em>{activity?.todo_completed ?? completed}/{total}</em>}</div>
      {todos.length > 0 ? (
        <><div className="agent-plan-progress"><span style={{ width: `${progress}%` }} /></div><ol>
          {todos.map((todo, index) => (
            <li className={`${todo.completed ? "completed" : ""} ${index === currentIndex ? "current" : ""}`} key={`${todo.text}-${index}`}>
              <span className="plan-marker">{todo.completed ? <Check size={12} /> : <span>{String(index + 1).padStart(2, "0")}</span>}</span><span>{todo.text}</span>
            </li>
          ))}
        </ol></>
      ) : <p className="agent-plan-empty">No plan has been published for this run yet.</p>}
    </div>
  );
}
