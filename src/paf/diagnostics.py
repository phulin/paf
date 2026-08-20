from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

LEAN_ERROR_RE = re.compile(r"(?m)^[ \t]*error:[ \t]*(?P<message>.*)$")
LEAN_WARNING_RE = re.compile(r"(?m)^[ \t]*warning:[ \t]*(?P<message>.*)$")
LEAN_SORRY_WARNING_RE = re.compile(r"(?:^|:\d+:\d+:[ \t]+)declaration uses [`']sorry[`'][ \t]*$")
LEAN_DIAGNOSTIC_RE = re.compile(r"^(?P<severity>error|warning):[ \t]*(?P<message>.*)$")
LAKE_CONTROL_PREFIXES = (
    "⚠ ",
    "✖ ",
    "✔ ",
    "trace:",
    "Some required targets logged failures:",
    "Coordinator rejected ",
)
FAILED_TARGETS_MARKER = "Some required targets logged failures:"
DIAGNOSTIC_TEXT_MAX_CHARS = 4_000


def _bounded_diagnostic_text(text: str) -> str:
    """Bound persisted diagnostic bodies; the raw build log remains authoritative."""

    if len(text) <= DIAGNOSTIC_TEXT_MAX_CHARS:
        return text
    marker = "\n... diagnostic body omitted; see raw_log_path ...\n"
    available = DIAGNOSTIC_TEXT_MAX_CHARS - len(marker)
    head = available // 2
    return text[:head] + marker + text[-(available - head) :]


@dataclass(frozen=True)
class LeanDiagnostic:
    """One actionable Lean diagnostic retained independently of display output."""

    severity: str
    header: str
    text: str

    def as_dict(self) -> dict[str, str]:
        return {"severity": self.severity, "header": self.header, "text": self.text}

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> LeanDiagnostic | None:
        severity = value.get("severity")
        header = value.get("header")
        text = value.get("text")
        if not isinstance(severity, str):
            return None
        if not isinstance(header, str):
            return None
        if not isinstance(text, str):
            return None
        return cls(severity, header, text)


def lean_diagnostics(output: str) -> tuple[LeanDiagnostic, ...]:
    """Extract errors and non-sorry warnings from a complete Lake transcript."""

    diagnostics: list[LeanDiagnostic] = []
    severity = ""
    header = ""
    lines: list[str] = []

    def finish() -> None:
        nonlocal severity, header, lines
        if not header:
            return
        text = _bounded_diagnostic_text("\n".join(lines).rstrip())
        if severity == "error" or unexpected_lean_warnings(header):
            diagnostics.append(LeanDiagnostic(severity, header, text))
        severity = ""
        header = ""
        lines = []

    for line in output.splitlines():
        match = LEAN_DIAGNOSTIC_RE.match(line)
        if match:
            finish()
            severity = match.group("severity")
            header = line.strip()
            lines = [line.rstrip()]
            continue
        if header and line.startswith(LAKE_CONTROL_PREFIXES):
            finish()
            continue
        if header:
            lines.append(line.rstrip())
    finish()

    # Validation may append a compact warning index after the original output.
    # Prefer the first occurrence because it retains the diagnostic body.
    unique: dict[str, LeanDiagnostic] = {}
    for diagnostic in diagnostics:
        unique.setdefault(diagnostic.header, diagnostic)
    return tuple(unique.values())


def failed_lean_modules(output: str) -> tuple[str, ...]:
    """Return module names from Lake's failed-target summary."""

    _, found, suffix = output.rpartition(FAILED_TARGETS_MARKER)
    if not found:
        return ()
    modules: list[str] = []
    for line in suffix.splitlines()[1:]:
        if match := re.fullmatch(r"-\s+([A-Za-z0-9_'.]+)", line.strip()):
            modules.append(match.group(1))
        elif modules:
            break
    return tuple(dict.fromkeys(modules))


def unexpected_lean_warnings(output: str) -> tuple[str, ...]:
    """Return Lean warning headers other than declarations that use ``sorry``.

    Lake renders cached and freshly built Lean diagnostics in the same
    ``warning: <location>: <message>`` form. Keep the exception intentionally
    narrow so warnings cannot disappear merely because their text mentions a
    sorry elsewhere in the diagnostic body.
    """

    warnings: list[str] = []
    for match in LEAN_WARNING_RE.finditer(output):
        message = match.group("message")
        if not LEAN_SORRY_WARNING_RE.search(message):
            warnings.append(match.group(0).strip())
    return tuple(warnings)


def lean_diagnostic_counts(output: str) -> tuple[int, int]:
    """Count error headers and non-sorry warning headers in streamed Lake output."""

    return len(LEAN_ERROR_RE.findall(output)), len(unexpected_lean_warnings(output))
