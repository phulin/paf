import { Box, Code2, Zap } from "lucide-react";
import type { DeclarationKind, LeanStatement } from "../../types";

export function KindIcon({ kind, size = 15 }: { kind: DeclarationKind; size?: number }) {
  if (kind === "theorem" || kind === "lemma") return <span className="kind-symbol" style={{ fontSize: size }}>⊢</span>;
  if (kind === "structure" || kind === "class") return <Box size={size} />;
  if (kind === "instance") return <Zap size={size} />;
  return <Code2 size={size} />;
}

function SyntaxLine({ line }: { line: string }) {
  const commentAt = line.indexOf("--");
  const code = commentAt >= 0 ? line.slice(0, commentAt) : line;
  const comment = commentAt >= 0 ? line.slice(commentAt) : "";
  const parts = code.split(/(\b(?:theorem|lemma|def|abbrev|structure|class|instance|namespace|section|variable|noncomputable|where|by|fun|match|with|let|in|if|then|else|have|show|from|exact|rw|simpa|using|Type|Prop)\b|:=|→|↔|∀|∃|λ|⊤|⊥)/g);
  return <>{parts.map((part, index) => {
    if (/^(theorem|lemma|def|abbrev|structure|class|instance|namespace|section|variable|noncomputable|where|by|fun|match|with|let|in|if|then|else|have|show|from|exact|rw|simpa|using)$/.test(part)) return <span className="syn-keyword" key={index}>{part}</span>;
    if (/^(:=|→|↔|∀|∃|λ|⊤|⊥)$/.test(part)) return <span className="syn-operator" key={index}>{part}</span>;
    if (/^(Type|Prop)$/.test(part)) return <span className="syn-type" key={index}>{part}</span>;
    return <span key={index}>{part}</span>;
  })}{comment && <span className="syn-comment">{comment}</span>}</>;
}

export function LeanCode({ statement }: { statement: LeanStatement }) {
  return (
    <div className="code-view" role="region" aria-label={`${statement.name} source code`}>
      {statement.excerpt.split("\n").map((line, index) => (
        <div className="code-line" key={index}><span className="line-number">{statement.line + index}</span><code><SyntaxLine line={line} /></code></div>
      ))}
    </div>
  );
}
