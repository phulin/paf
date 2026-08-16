from __future__ import annotations

from dataclasses import dataclass

from rich.text import Text

from paf.state import TokenUsage


@dataclass(frozen=True)
class ActivityKindDisplay:
    label: str
    color: str


ACTIVITY_KIND_DISPLAYS = {
    "agent": ActivityKindDisplay("agent", "#7aa2f7"),
    "usage": ActivityKindDisplay("tokens", "#bb9af7"),
    "todo": ActivityKindDisplay("plan", "#e0af68"),
    "message": ActivityKindDisplay("msg", "#7dcfff"),
    "reasoning": ActivityKindDisplay("think", "#9d7cd8"),
    "command_execution": ActivityKindDisplay("bash", "#2ac3de"),
    "file_change": ActivityKindDisplay("edit", "#9ece6a"),
    "mcp_tool_call": ActivityKindDisplay("mcp", "#f7768e"),
    "collab_tool_call": ActivityKindDisplay("swarm", "#ff9e64"),
    "web_search": ActivityKindDisplay("web", "#73daca"),
    "error": ActivityKindDisplay("error", "#db4b4b"),
    "context_compaction": ActivityKindDisplay("compact", "#c0caf5"),
    "dynamic_tool_call": ActivityKindDisplay("tool", "#b4f9f8"),
    "image_generation": ActivityKindDisplay("image", "#e0aaff"),
    "image_view": ActivityKindDisplay("view", "#fca7ea"),
    "sub_agent_activity": ActivityKindDisplay("subagent", "#c3e88d"),
    "hook_prompt": ActivityKindDisplay("hook", "#ffc777"),
    "entered_review_mode": ActivityKindDisplay("review+", "#89ddff"),
    "exited_review_mode": ActivityKindDisplay("review-", "#82aaff"),
    "user_message": ActivityKindDisplay("user", "#f78c6c"),
    "extension": ActivityKindDisplay("ext", "#c792ea"),
}

ACTIVITY_KIND_ALIASES = {
    "collab_agent_tool_call": "collab_tool_call",
    "compaction": "context_compaction",
}


def activity_kind_display(kind: str) -> ActivityKindDisplay:
    canonical = ACTIVITY_KIND_ALIASES.get(kind, kind)
    if display := ACTIVITY_KIND_DISPLAYS.get(canonical):
        return display
    label = kind.replace("_", "-")
    if len(label) > 12:
        label = f"{label[:11]}…"
    return ActivityKindDisplay(label or "event", "#a9b1d6")


def activity_kind_badge(kind: str) -> Text:
    display = activity_kind_display(kind)
    return Text(f"[{display.label}]", style=f"bold {display.color}")


def format_count(value: int) -> str:
    if value < 1_000:
        return str(value)
    if value < 1_000_000:
        return f"{value / 1_000:.1f}k"
    if value < 1_000_000_000:
        return f"{value / 1_000_000:.2f}m"
    return f"{value / 1_000_000_000:.2f}b"


def format_usage(usage: TokenUsage, *, label: str = "Tokens") -> str:
    if not usage.measured:
        return f"{label}: awaiting measured usage"
    return (
        f"{label}: {format_count(usage.total_tokens)}  "
        f"input {format_count(usage.input_tokens)} "
        f"(cached {format_count(usage.cached_input_tokens)})  "
        f"output {format_count(usage.output_tokens)}  "
        f"reasoning {format_count(usage.reasoning_output_tokens)}"
    )
