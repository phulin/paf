import { ChevronDown } from "lucide-react";
import type { CSSProperties, ReactNode } from "react";

export function IconButton({
  label,
  children,
  onClick,
  active = false,
}: {
  label: string;
  children: ReactNode;
  onClick?: () => void;
  active?: boolean;
}) {
  return (
    <button className={`icon-button ${active ? "active" : ""}`} onClick={onClick} title={label} aria-label={label}>
      {children}
    </button>
  );
}

export function ProgressBar({ value, color }: { value: number; color?: string }) {
  return (
    <div className="progress-track">
      <div className="progress-fill" style={{ width: `${Math.min(100, Math.max(0, value))}%`, background: color } as CSSProperties} />
    </div>
  );
}

export function Select({
  value,
  onChange,
  children,
  label,
}: {
  value: string;
  onChange: (value: string) => void;
  children: ReactNode;
  label: string;
}) {
  return (
    <label className="select-wrap" title={label}>
      <select value={value} onChange={(event) => onChange(event.target.value)} aria-label={label}>{children}</select>
      <ChevronDown size={14} />
    </label>
  );
}
