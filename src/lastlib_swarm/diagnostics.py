from __future__ import annotations

import re

LEAN_ERROR_RE = re.compile(r"(?m)^[ \t]*error:[ \t]*(?P<message>.*)$")
LEAN_WARNING_RE = re.compile(r"(?m)^[ \t]*warning:[ \t]*(?P<message>.*)$")
LEAN_SORRY_WARNING_RE = re.compile(
    r"(?:^|:\d+:\d+:[ \t]+)declaration uses [`']sorry[`'][ \t]*$"
)


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
