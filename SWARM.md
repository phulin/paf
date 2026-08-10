# LastLib Swarm

`lastlib-swarm` orchestrates many noninteractive Codex workers over a corpus of informal books and
Lean chapters. It supports individual stages and a full, resumable formalize → review → prove ↔
repair pipeline. Chapter work is isolated by configured file scopes, and book dependencies provide
coarse ordering without sacrificing chapter-level parallelism.

The checked-in [`swarm.example.toml`](swarm.example.toml) targets Book 2. Copy it to `swarm.toml`,
adjust the agent count and model, then install the CLI with:

```console
uv sync
uv run lastlib-swarm plan --config swarm.toml
```

The full command reference and state/TUI details will live here as the implementation is completed.
