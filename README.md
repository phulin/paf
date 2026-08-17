# PAF

`paf` orchestrates a large population of noninteractive Codex workers over an informal
mathematics corpus and its Lean translation. It combines parallel source-dependency discovery,
dependency-ready formalization with clean diagnostics, editing mathematical review, and LSP-backed
proving.

Install `ripgrep` and ensure its `rg` executable is on `PATH` before starting a large swarm. Agents
use it for fast source and declaration searches. Worker-launching commands continue without it, but
print a prominent warning in headless/background mode or retain a warning banner in the TUI because
fallback searches can be substantially slower.

```mermaid
flowchart LR
    S[Scaffold directories] --> D[Discover source tree per input]
    D -->|own discovery + formalized dependencies| F[Formalize with MCP]
    F -->|clean diagnostics and build| G{First review dependencies done?}
    G --> R[Editing review]
    R -->|successful own review| P[Prove or revalidate proof]
    R -->|changed, findings, or build failure| R
    R -->|succeeded| P
    P -->|statement/API problem; reopen affected reviews| R
    P -->|missing earlier interface| UQ[Upstream request: requested]
    UQ --> UR[Temporary owner repair run]
    UR -->|exact answer persisted| UA[answered]
    UA --> UT[Fresh consumer proof run]
    UT -->|named declaration proved + Lean valid| UC[closed]
    UC --> P
    UQ -->|invalid request| UM[escalated]
    UR -->|no clean durable answer| UM
    UT -->|named declaration still blocked| UM
    P -->|no placeholders + Lean valid| D[Done]
```

Scaffolding is deterministic and creates directories only. Discovery reads each input chapter in
parallel, identifies its direct source prerequisites, and persists the resulting source dependency
tree with source digests. A chapter can formalize as soon as its discovery is complete and its
dependencies have formalized; it does not wait for unrelated discovery or formalization work.
Formalization owns full source coverage, elaboration, MCP diagnostics, and coordinator build
convergence. It permits only the exact declaration-uses-`sorry` warning.
All later coordinator checks use the same coalescer: formalization, review, proof refresh, and proof
certification requests that are pending together contribute targets to one Lake command, even
across stages. The highest-priority request supplies the batch priority, and any non-preemptible
request makes the shared command non-preemptible. If the command fails, diagnostics are routed to
their owning chapters and affected import descendants while independent targets are retried as one
smaller batch and retain their successful freshness records.
A first review starts once its own formalization is clean and all source-tree dependencies have been
reviewed. A later targeted or forced re-review does not wait for dependency reviews. There is no
corpus-wide barrier between formalization and review. The coordinator remembers the source
digests of successful builds so it can avoid rebuilding unchanged chapters. Review has
no separate green flag: a successful review task is the whole state. Restarts and ordinary proof-body edits leave it
succeeded; explicit review findings, proof-requested statement/API repairs, and forced review reopen
only the reviews that receive findings. Reviewers directly make scoped statement and API repairs.
If a review edits source, transitive build freshness is invalidated and downstream proofs are
kept green when a fresh coordinator build still succeeds. Dependency-ready chapters receive prioritized, coalesced coordinator
verification after changed reviews; failures and structured findings return to review with the
diagnostics attached. After at most five
such cycles—or immediately after a no-change review—the chapter is
released to proving without waiting for reviews of its descendants.
Proof edits are validated by building their own chapter, but they do not invalidate or proactively
rebuild downstream chapters. Build freshness remains separate from review and proof task status.
Proof findings reopen review without invalidating build freshness; a subsequent review edit is what
marks the affected import closure stale.
Review agents read the assigned numbered textbook chapter and discover the assigned Lean files
dynamically, then use targeted searches in earlier LastLib and pinned Mathlib sources as questions
arise. They do not receive a prefabricated source packet. Formalize, review, and proof agents receive Lean MCP;
reviewers trust the last clean build for untouched files and request whole-file diagnostics
only for files they edit and the assigned transitive dependents invalidated by those edits. A
no-change review therefore needs no diagnostic calls.

Point the CLI at source files or directories. Directories are scanned recursively for Markdown,
LaTeX, and plain-text documents; metadata directories, hidden directories, build outputs, and
symlinked directories are skipped. A conventional numbered corpus also automatically reads
`BOOK_DEPENDENCIES.md` from the repository root:

```console
uv run paf plan books/
uv run paf corpus books/
```

When `corpus` discovers a `paf.toml` in the target directory or one of its ancestors, that
configuration controls source discovery and target mapping. Without a discovered configuration,
PAF infers the corpus from the positional files or directories. Pass `--target` to request an
explicit zero-config output mapping.

Dependency documents use Mermaid edges such as `B01 --> B02 --> B03`; chained edges are expanded.
Pass `--dependencies path/to/graph.md` to select another graph. Dependencies whose books are outside
the selected target set are treated as already satisfied.

## Quick start

The project is managed with [uv](https://docs.astral.sh/uv/). A config file is optional: pass one
informal book Markdown file and PAF infers its title, numbered chapters, existing matching
Lean book module, validation commands, and isolated state directory.

```console
uv sync --all-groups
uv run paf plan books/02-finite-extensions-of-local-fields.md
uv run paf books/02-finite-extensions-of-local-fields.md
```

Install the command from a checkout with:

```console
uv tool install .
```

The distribution name is `paf`; after a release is published to the configured Python package
index, install that published release with:

```console
uv tool install paf
```

The distribution includes the prompt library and a prebuilt React UI. A normal wheel or sdist
installation does not require Node or npm.

Serve the installed dashboard and its project-scoped API from any directory with:

```console
paf web /absolute/path/to/project
```

It listens on `127.0.0.1:5173` by default. Network exposure is opt-in: pass
`--host 0.0.0.0` explicitly (and choose a different port with `--port`). The service reads source,
target, and durable run state only from the paths resolved by that project's `paf.toml`, including
an external `state_dir`.

Passing a `.md` as the first argument is shorthand for `pipeline <target>`. Zero-config runs default
to `gpt-5.6-luna`, reasoning effort `max` (with discovery using `gpt-5.6-luna` at `medium`), the packaged generic prompt library under
`src/paf/prompts/`, automatic execution isolation, and a state directory at
`.paf/<inferred-book-id>/`. Fixup, review, and proof attempts attach a private Lean LSP MCP server;
drafting does not. Use
`paf.example.toml` when the inferred layout is not appropriate or when coordinating multiple books.

Any stage may point at a specialized prompt template. Supported replacement fields include
`{book_title}`, `{chapter_number}`, `{chapter_number_padded}`, `{chapter_title}`, `{source}`,
`{lean_root}`, `{chapter_path}`, `{chapter_module}`, and `{build_command}`.

The final agent prompt is composed from three deliberately separate layers: the selected stage
template defines its mission and workflow, packaged `prompts/common.md` defines shared mathematical
and import policy, and the generated runtime contract supplies the exact scope, MCP availability,
coordinator-owned build behavior, and structured-report requirements. Shared policy belongs in the
common layer rather than being copied into every stage template.

Proof agents must clear every MCP warning except “declaration uses `sorry`” for a theorem they
attempted but could not prove. Disabling a linter or warning option is not an acceptable fix.

Codex is invoked in its documented noninteractive JSONL mode with a strict final-report schema.
Every worker returns a nonempty, change-focused `summary`. For an accepted source change, the
coordinator copies that summary verbatim into the body of a path-scoped Conventional Commit such as
`chore(book07): changes from review agent on book 7 chapter 2`, and records the resulting commit SHA
with the run. The coordinator refuses to launch a worker whose exclusive scope already contains
uncommitted files, preventing earlier work from being folded into the worker's commit. Unrelated
staged and unstaged files remain outside the commit.
All swarm workers, including review workers, default to
`--dangerously-bypass-approvals-and-sandbox`, giving them full host access for unattended
formalization. Discovery runs directly in the canonical repository without allocating an isolated
workspace and its instructions prohibit edits. The coordinator accepts review changes only inside
the chapter's exclusive scope. To
opt into sandboxed workers, set `bypass_approvals_and_sandbox = false`; setting
`approve_for_me = true` then uses Codex's workspace-write sandbox for every mutating stage. See
OpenAI's official
[Codex non-interactive guidance](https://developers.openai.com/codex/noninteractive).

Transient model-capacity failures resume the same Codex thread with capped exponential backoff.
After the configured retries are exhausted, that stage fails instead of creating an unbounded stream
of fresh attempts. A failed chapter does not cancel unrelated formalizers; the pipeline finishes the
remaining drafting work before reporting failure.

Pass `--resume` to `pipeline`, `stage`, `corpus`, `agent start`, or `agent serve` to continue work
interrupted by an earlier orchestrator shutdown in its saved Codex sessions. Every restart requeues
interrupted tasks; without `--resume`, they start fresh agents. With `--resume`, the coordinator first
tries each saved session id and transparently starts a fresh agent if Codex can no longer resume it.

## Commands

Inspect discovery and configuration without launching agents:

```console
uv run paf plan books/02-finite-extensions-of-local-fields.md
uv run paf plan --config paf.toml
```

Run an individual stage over the whole configured corpus or a selection:

```console
uv run paf stage formalize books/02-finite-extensions-of-local-fields.md
uv run paf stage discover books/02-finite-extensions-of-local-fields.md
uv run paf stage review --config paf.toml --book book02
uv run paf stage prove --config paf.toml --book book02 --chapter 3
```

Create the deterministic directory scaffold without launching Codex:

```console
uv run paf scaffold books/02-finite-extensions-of-local-fields.md
```

Run the complete pipeline:

```console
uv run paf pipeline books/02-finite-extensions-of-local-fields.md
uv run paf corpus books/ --max-agents 24
uv run paf corpus books/ --target lean/Stacks/
```

For a zero-config corpus, `--target` selects the generated Lean namespace root. PAF locates the
enclosing Lake project and derives the module prefix from the target path, so the last command
above writes inferred book modules beneath `lean/Stacks/` with the `Stacks` namespace.

The native Rust TUI is the default for stage and pipeline runs. It runs as a separate process from
the Python orchestrator and consumes the same bounded dashboard model as the web UI over a
versioned Unix-socket protocol. The server sends one initial snapshot, then pushes coalesced task,
agent-activity, and global-state deltas directly from the in-process change bus; the TUI does not
poll SQLite or repeatedly request full snapshots. Press `Enter` or `i` to inspect the selected
agent, `p` to pause or resume scheduling, and `q` to stop workers, integrate interrupted workspace
changes, and return to the shell. Press `d` to detach the TUI while leaving a managed orchestrator
running.

Add `--no-tui` for CI, a process supervisor, or log-only operation. Add `--force` to rerun tasks
already persisted as successful. Without `--force`, successful stages are resumable and skipped.

Inspect durable status without starting or mutating a run:

```console
uv run paf status books/02-finite-extensions-of-local-fields.md
uv run paf status --config paf.toml --json
```

Inspect the accumulated informal-textbook issue ledger:

```console
uv run paf source-issues books/02-finite-extensions-of-local-fields.md
uv run paf source-issues --config paf.toml --json
```

Every Lean-writing agent report has a structured `source_issues` field. Genuine textbook defects are recorded
with a precise location, exact identifying excerpt, mathematical explanation, and minimal suggested
replacement. The coordinator enriches each sighting with book, chapter, stage, and run provenance,
deduplicates repeated sightings, and persists the ledger in normalized SQLite state. An explicitly
exported snapshot includes the same records. Detection does not stop an agent: it must make the
principled accommodation
allowed by its stage and continue through all unaffected work. The ledger is evidence for a later
reviewed textbook patch; swarm workers do not rewrite the Markdown automatically.

`--chapter N` selects that chapter number in every selected book. Use the full chapter id when a
number would be ambiguous.

CLI overrides such as `--model`, `--reasoning-effort`, `--max-agents`, and
`--discover-max-agents` work with both inferred and configured runs.
`--isolation auto|fuse-overlay|shared` selects the execution backend.

After a foreground TUI closes with a failed result, the CLI prints the failed task details, compact
agent/build diagnostics, blocked dependents, and persisted state path to standard output.

## Frontend release bundle

Rust is compiled into release wheels by Maturin. Contributors installing from a source checkout
therefore need a Rust toolchain; users installing a wheel do not. Node is needed only by
contributors rebuilding the React app. After changing `web/src`, frontend
configuration, or npm package metadata, prepare and verify the package assets with:

```console
cd web
npm ci
npm run release:bundle
cd ..
python scripts/web_bundle.py check
```

The release command writes content-hashed assets and a content manifest under the ignored
`src/paf/web_dist/` directory. Generated frontend files are not committed to Git. The check compares
SHA-256 digests rather than filesystem mtimes, so it detects stale sources and edited build output.
Build the bundle before creating a wheel or source distribution, then validate the package contents:

```console
uv build
python scripts/check_distribution.py dist/*.whl dist/*.tar.gz
```

The full installed-package check builds both archive types, inspects their contents, installs each
one into a separate temporary virtual environment, and exercises the CLI and live web server from
outside this checkout:

```console
uv run python scripts/check_installed_distribution.py
```

It removes `PYTHONPATH`, disables user site packages, and verifies the imported `paf` location, so
the checkout cannot accidentally satisfy an installed-package probe.

## Lean LSP MCP proof loop

Every proof attempt receives its own locked
[`lean-lsp-mcp`](https://github.com/oOo0oOo/lean-lsp-mcp) stdio server, rooted inside the agent's
private FUSE overlay. It exposes whole-file diagnostics, hover and declaration lookup,
local source search, proof goals, completions, code actions, batched tactic trials, and focused
dependency preparation. Remote search, local Loogle, and the MCP's general `lean_build` tool remain
absent from the allowlist.

Proof agents first read and attempt the entire assigned file set without checking each speculative
proof separately. They then request whole-file diagnostics and iterate only over failing proofs and
their dependent declarations. The MCP opens files against the certified cache without requesting a
build and retains warm file workers across ordinary body edits. A stale-import notification,
stale-import diagnostic, or import-header edit enters one per-server preparation coordinator: it
serializes the actual `lake setup-file` work through the freshness barrier, retries the query once,
and lets queued files first test the cache produced by the preceding preparation. Repeated failed
preparations are suppressed until source or cache state changes.

For a multi-file changed closure, agents prepare only its maximal affected dependents before the
final import-order diagnostic pass. This builds the imported closure in the fewest Lake invocations;
preparing every file separately is forbidden. Files are opened lazily on the first Lean tool call,
and batched tactic trials use one scratch worker per MCP server. Agents never invoke a compiler
directly. After an agent exits, the coordinator terminates its MCP/LSP process group and merges
accepted scoped changes into the main worktree under a short source-consistency barrier. It then
enqueues one visible targeted `lake build +Module`; only Lake builds acquire the prioritized
coordinator build queue. Review and formalize verification outrank proof certification, and an active
proof certification is cancelled and requeued when statement-critical build work arrives. Each
coordinator build receives a private cache upper layer. A successful build atomically promotes that
small delta and writes only those changed artifacts back to the main worktree's `.lake`; failed and
preempted build deltas are discarded.
The coordinator rejects a successful compiler exit if its captured output contains any warning
other than the exact “declaration uses `sorry`” diagnostic. This check reuses the targeted build and
does not launch a second compiler.

An MCP/LSP process exists per active proof attempt, not per queued agent. Its imported `.olean` files come
from a read-only snapshot of the coordinator cache taken when the attempt starts, while its document
state remains private. No new attempt snapshot is created during a merge/build transaction, so it
cannot pair newly merged sources with stale artifacts. Consequently `max_agents` also bounds the
number of concurrent Lean language servers. Discovery uses the separate
`stages.discover.max_agents` pool and does not consume these mutating-agent slots.

## Agent-managed background mode

Managing agents do not need to hold open a TUI or manipulate a background process's stdin. Start a
detached pipeline, then issue one-shot Bash commands over its Unix control socket:

```console
TARGET=books/02-finite-extensions-of-local-fields.md
uv run paf agent start "$TARGET"
uv run paf agent status "$TARGET"
uv run paf agent pause "$TARGET"
uv run paf agent resume "$TARGET"
uv run paf agent unblock "$TARGET"
uv run paf agent snapshot "$TARGET"
uv run paf agent snapshot "$TARGET" --output snapshot.json
uv run paf agent inspect "$TARGET" --chapter 8
uv run paf agent inspect "$TARGET" --chapter book02/chapter-08 --follow
uv run paf agent wait "$TARGET"
```

`TARGET=books` manages the inferred corpus through the same detached protocol; the directory and
auto-discovered dependency graph deterministically select a corpus-specific state directory.

Every command prints one JSON object. `wait` blocks until completion and exits with the pipeline's
success/failure code. `snapshot` includes the complete persisted task/run state; `status` is compact.
`snapshot --output PATH` additionally writes that complete state to JSON atomically. No JSON file is
written unless an output path is explicitly supplied.
Use `agent stop` to cancel the scheduler and terminate active Codex/build subprocess groups.

Pause is cooperative: already-running chapter attempts finish, while new agent/build attempts wait at
a checkpoint. This preserves coherent edits and build results. Resume releases all queued work.
`unblock` resets every persisted `blocked` task to `pending` without deleting its run history. It can
be issued against either a live daemon or offline state; pending tasks are eligible the next time the
corresponding stage is scheduled. For a manually escalated upstream proof request, it also reopens
the durable handoff while retaining any existing answer and all repair/retry evidence.

For a long-lived managing agent, the `rpc` facade accepts newline-delimited JSON commands on stdin and
returns one JSON response per line:

```console
printf '%s\n' '{"command":"status"}' '{"command":"snapshot"}' \
  | uv run paf agent rpc "$TARGET"
```

Accepted RPC commands are `status`, `snapshot`, `pause`, `resume`, `unblock`, `stop`, `wait`, and
`inspect`.
Inspection requests may include `chapter` or `run`, for example
`{"command":"inspect","chapter":"book02/chapter-08"}`. The daemon
stores its socket, PID, JSON result, and combined stdout/stderr log under the inferred state directory.
After the daemon exits, `status`, `snapshot`, and `wait` fall back to the persisted offline result.
`agent serve` exposes the same protocol in the foreground for an external process supervisor.

## Fixed-point semantics

- **Scaffold** deterministically creates the configured chapter directories and no Lean files.
- **Discover** reads every selected input directly from the canonical repository, reports direct
  source prerequisites by work-unit id, and persists the source dependency
  tree and input digests. Independent discoveries run concurrently without allocating overlays.
- **Formalize** starts after its own discovery and the formalization of its direct dependencies.
  It covers the complete source chapter, uses Lean MCP for elaboration and diagnostics, and cycles
  through coordinator builds up to `max_rounds`. Completion requires a clean build with no warning
  other than `declaration uses sorry`.
- **Review** visits dependency-ready chapters against a clean source and `.olean` generation. Only
  the first review waits for source-tree dependency reviews; subsequent re-reviews do not. Agents
  make warranted in-scope statement and API changes directly; exact out-of-scope blockers remain in
  the report. The last clean coordinator build remains authoritative for
  files a reviewer does not edit. Reviewers request whole-file LSP diagnostics after the last relevant
  edit only for the edited files and their assigned transitive dependents, rechecking just the files
  invalidated by any subsequent repair. A changed chapter receives a prioritized coordinator build.
  Failed builds and structured proof `failed_attempts` are fed to a full-scope re-review of the
  affected chapter; they return to review rather than formalization.
  An initial review that edits source receives another full pass until one pass is clean, capped at
  five review/verification cycles. A review launched with specific persisted findings instead makes
  one repair pass and completes as soon as its coordinator rebuild is clean; it does not require a
  redundant no-change confirmation pass. Once a chapter is clean, its proof agent may start while
  descendant reviews continue. A reported chapter-local failure is quarantined: unrelated review and
  proof branches keep running, while actual dependents are marked blocked. Unexpected coordinator
  exceptions still fail fast after draining live workers. There is no corpus-wide clean-build or
  review gate in the pipeline. Successful completion leaves the review task `succeeded`. Forced
  review moves all selected reviews back. Already-running downstream agents finish against their
  pinned clean snapshot instead of being cancelled. Downstream reviews and proofs remain green unless
  an actual source edit makes their coordinator build fail. Ordinary proof edits and restart
  reconciliation do not reopen review.
- **Prove** sends one chapter to an agent and asks it to work directly on unresolved placeholders.
  A cumulative attempt ledger is prior inventory: later agents must refine an earlier proof using
  new evidence or try a materially different route instead of rereading the chapter, reconfirming
  clean diagnostics, or echoing a missing-API claim. Two consecutive rounds without reducing the
  placeholder count stop the proof loop. The stage succeeds only when the scoped Lean code has no
  `sorry` or `admit` tokens and final Lake validation succeeds without any warning other than
  “declaration uses `sorry`”. The exact validated source digest is persisted independently of review
  status. If it is stale after restart, the coordinator rebuilds and recounts placeholders first; a
  successful placeholder-free source is accepted without launching another proof agent. Proof
  validation uses the same visible coordinator-build record at lower priority; it never holds the
  build queue while the proof agent edits, takes a snapshot, or merges its scoped patch.
- If sustained checked proof work exposes a genuinely missing earlier interface, the proof agent
  records the blocked declaration and consumer path, exact residual goal, minimal result needed,
  proposed owner chapter and paths, and at least two materially different attempted alternatives.
  It keeps working on independent declarations. The coordinator persists the request before doing
  anything else and batches all requested records by owner chapter. A targeted variant of the
  failed-proof re-review receives the complete batch, consumer declaration excerpts, residual goals,
  prior attempts, upstream source paths, and relevant textbook excerpts. Owner and ordinary chapter
  agents share the same per-chapter lock, while this auxiliary run leaves the owner's ordinary proof
  task and round count untouched. A clean repair either adds and proves the interface, identifies an
  existing exact declaration, or records why the bridge belongs downstream. Reported additions and in-corpus
  declarations are checked against the integrated placeholder-free sources before their answers are
  accepted. The request remains `requested` while this agent runs and becomes `answered` only after
  that completed answer is durably stored.
- Every accepted answer is retained in durable state, including exact declaration names,
  application guidance, or the downstream-placement rejection. The consumer then gets exactly one
  fresh targeted proof agent with the original request, durable answer, and previous attempt ledger.
  The request remains `answered` while that retry runs.
  Only a clean coordinator build in which the named blocked declaration no longer contains a
  placeholder closes the request. A malformed request, failed owner repair, unusable answer, or failed
  targeted retry blocks the proof task and marks the request `escalated`; ordinary `unblock` explicitly
  authorizes another targeted attempt without erasing run history.
- A proof agent may change proof bodies but not declaration interfaces. It reports each unresolved
  proof through structured `failed_attempts`, including checked approaches and the exact remaining
  goal; the pipeline sends that evidence to an independent editing re-review before proving resumes.

The orchestrator independently hashes every configured chapter scope. Agent claims about changes do
not control review convergence. Lean placeholder scanning ignores comments and strings.

## Multi-book scheduling

The corpus layer validates the book graph as a DAG before launching any agent and reports an explicit
cycle path on failure. It computes a weighted **bottom level** for every book:

```text
rank(book) = effort(book) + max(rank(successor), default=0)
```

Whenever several dependency-ready books compete for a limited agent slot, the scheduler chooses the
largest rank first. This is critical-path list scheduling: it directs capacity toward the longest
remaining dependency chain while still filling spare slots with independent work. It is a heuristic
for parallel machines—not a claim of an optimal schedule for arbitrary runtimes—but it directly
targets critical-path completion and adapts as dependencies unlock.

Effort defaults to the number of selected chapters. Configured corpora can supply positive
`statement_effort` and `proof_effort` values based on historical wall time or expected agent work.
Ranks prioritize competition for agent slots; optimistic drafting is not dependency-gated. The plan,
TUI, persisted snapshot, and agent-control JSON expose the priority order, ranks, and critical path.

## TUI and token accounting

The dashboard shows:

- the live-agent total against the combined discovery and mutating limits, broken down by stage and
  concurrency pool;
- the active coordinator build's mode, stage, iteration, target progress, current chapter, owner, and
  queued build count;
- Shepherd status, pending failure count, and planned/running repair units;
- aggregate `pending`, `queued`, `running`, `succeeded`, `failed`, `blocked`, and `interrupted`
  chapter counts for discover, formalize, review, and prove;
- each chapter's status and attempt count in every stage, plus independent exact-build freshness;
- per-chapter tokens for the current invocation;
- statement/proof critical-path ranks and the current statement critical path;
- each running agent's current command, MCP call, edit, or reasoning state and failure count;
- measured cumulative input, cached input, output, and reasoning-output tokens.

Select a chapter row and press Enter (or `i`) to open its agent detail screen. It updates while the
agent runs and has tabs for the event timeline, exact submitted prompt, Codex todo plan, touched
files, and bounded raw JSONL events. Opening a run reconstructs its timeline from JSONL so the whole
run is visible; only an extreme run beyond 10,000 display events is shortened, with the omission
called out. The header includes PID/thread id, elapsed and idle time, shell/MCP/edit counts,
complete latest agent update in a scrollable pane, latest substantive error, and measured per-run
spend. Bare process statuses such as `exit 2` are retained in the event timeline but are not promoted
to latest or systemic errors. The detail pane reports tokens and API-equivalent cost for that exact
agent attempt, and the overview-only Inspect action is hidden there. Press Escape or `q` to return
to the swarm overview. Repeated equivalent failures across agents produce a deduplicated systemic
alert in the overview instead of requiring inspection of every chapter. Press `d` from either view
to detach without sending a stop request to the orchestrator.

Codex JSONL, state, activity, and control messages are decoded with `orjson`. Live JSONL is still
framed incrementally rather than loaded as a whole file. Dashboard cells and static cards are updated
only when their values change, and the raw-event tab tails newly appended bytes instead of reparsing
the log on every refresh.

The token count means `input_tokens + output_tokens`. The primary dashboard and CLI total is for
attempts started by the current swarm invocation; a separately labelled lifetime total includes
every persisted attempt in this state directory, including failed and cancelled attempts. Both views
also show the corresponding API-equivalent dollar cost for the model recorded on each attempt.
Legacy attempts created before model persistence are priced as `gpt-5.6-luna`.
`cached_input_tokens` is displayed separately but is already a subset of input, so it is not added a
second time. Reasoning output is also shown separately and is not double-counted.

Codex's stdout JSONL normally reports authoritative usage only when a turn completes. Once Codex
reports a thread id, the orchestrator also tails that thread's local rollout token-count events and
updates the in-memory cumulative usage. These frequent updates are folded into the next state batch;
cancellation drains the rollout once more and force-flushes that partial spend. The final stdout
usage, when available, updates the same record. If a Codex version does not emit recognized usage
fields in either stream, the TUI says usage is awaiting measurement rather than inventing an
estimate.

## State, logs, and interruption

Configured state defaults to `.paf/`; inferred single-target state uses `.paf/<book-id>/`, and
inferred corpora use a deterministic `.paf/corpus-<id>/`. `state.sqlite3` is the canonical WAL
database. Documents, work units, tasks, runs, globals, and issue/request records occupy normalized
rows. Mutations enqueue immutable deltas to one background writer, which coalesces a short window and
updates only changed rows. Immutable run payloads are loaded only for inspection or a full `snapshot`
request. JSON is an explicit interchange/export format only: `paf agent snapshot --output PATH`
writes a complete snapshot, while startup, live transitions, and clean shutdown touch only SQLite.

On the first load of a pre-SQLite state directory, the orchestrator imports every run and source
issue in one transaction, verifies that the database is complete, and retains a backup of the original
snapshot as `state.legacy-v6.json`. It does not modify or delete the original `state.json`. Restarting an
interrupted run updates its lightweight row summary without discarding its lazily stored report,
validation, or isolation payload.

Raw JSONL agent logs and their exact submitted `.prompt.md` sidecars live below `logs/`, with the
generated final-report schema alongside them.
Compact `*.activity.json` sidecars retain the recent event timeline and health counters without
copying command output into pipeline state; because they are reconstructible from JSONL, event-time
sidecar writes are rate-limited and a final summary is force-flushed. An attempt row is committed
before its workspace is acquired or Codex is launched. Each run records its PID, Codex thread id,
stage, round, timestamps, scoped-change result, placeholder count, final report, validation tail, and
usage. Lifecycle transitions await a durability ticket; high-frequency usage is coalesced for 500 ms.
Each transaction advances a bounded revision/change feed used by status and web readers. The TUI
subscribes to the same in-process changes and recomputes only affected rows and aggregate cards;
the web dashboard bootstraps once, then merges only changed work-unit rows, global projections, and
bounded active-run activity. A stale web cursor or static corpus change requests one full resync.
Repository hashing, JSON export, and database work run off the TUI event loop. Task records persist one
status (`pending`, `running`, `succeeded`, `failed`, `blocked`, or `interrupted`) plus a short detail
describing what
a running or pending task is doing. A transient `queued` marker distinguishes runnable pending stages
that are waiting for an agent slot, and both the TUI and web dashboard label them accordingly.
An independent persisted `repairing` overlay and repair-work-unit id identify the exact stage cell
currently owned by a Shepherd worker without mutating that task's ordinary state machine. An active
coordinator validation build takes display precedence and labels that cell `building`.
Successful discovery reports enter a short bounded batch: PAF merges the reports, rebuilds the source
dependency graph once, and persists the graph plus all task promotions atomically. At most twice the
configured discovery-agent pool is scheduled at once. Dashboards count live run records as working
agents. Running tasks persist an explicit `agent` or `postprocess` phase, so the TUI and web dashboard
show completed agent work awaiting integration, graph persistence, or coordinator verification as
postprocessing.
Statement repair requests are checkpointed before entering the
in-memory batching queue and removed only after their dependency-aware review pass returns. Upstream
proof requests instead remain in the checkpoint permanently and record only completed facts:
`requested`, `answered`, `closed`, or `escalated`. Their owner-grouped requested batches are exported
in the snapshot. Active repair and retry work exists only as ordinary persisted run records. Because a
request changes state only after that work completes, generic interrupted-run recovery naturally
leaves an interrupted repair `requested` and an interrupted consumer retry `answered`; no separate
request-state rewind is needed. Recovery also reconstructs both a handoff from a completed proof report
when the process stopped between those two durable writes and proof-requested statement repairs written
by older orchestrators that invalidated review state before checkpointing the handoff. The chapter table
displays exact-build
freshness independently of whether a past formalize task succeeded. A coordinator-build record tracks the
single serialized Lake build, and the TUI also shows its owner and queued jobs. Running run records—not
chapter-stage records—are the authoritative live-agent count.

Press `q` in the TUI or interrupt a headless run to terminate the active child process group. Active
runs and their tasks become `interrupted`, never `failed` or dependency-propagated `blocked`, and
retain the Codex session id observed before shutdown. The next invocation resets those tasks to
`pending` and starts fresh agents by default. With `--resume`, it instead tries the saved sessions and
falls back to fresh agents when a session is no longer available. Successful records remain skipped
unless `--force` is given. The TUI drains its
workers and unmounts their overlays
before exiting. Shutdown waits briefly for the complete Codex process group and force-terminates
surviving MCP/LSP descendants before unmounting; the next invocation also reclaims any mounts left
by a hard-killed orchestrator.

Agents never commit. With the default `auto` backend, supported Linux systems run each mutating
attempt in a private `fuse-overlayfs` view. Discovery is read-only and runs against the canonical
repository without a private worktree. Each isolated view has two immutable lower generations: a
source snapshot that excludes `.lake`, and an ordered manifest of read-only coordinator cache
layers. The package cache
is seeded once per invocation in a dependency layer keyed by `lean-toolchain` and
`lake-manifest.json`; project artifacts begin in a separate layer. Only files changed by that
attempt occupy its writable upper layer. Codex, its private LSP, and coordinator validation run in
private merged views. The coordinator rejects every out-of-scope source path, checks that the assigned
canonical scope has not changed since launch, and imports accepted regular files with atomic
replacement. A stale writer is
discarded and retried by the normal stage loop.

The number of mounted workspaces is bounded by `max_agents`; slots and upper layers are removed after
each attempt. Generations share unchanged file data through hard links and are removed when their last
reader exits. Temporary mounts live outside the repository so Git discovery starts from the private
view.

All proof agents pinned to a cache-generation manifest share the same Lake artifact inodes and OS
page-cache pages; neither the complete cache nor its dependency packages are copied after each
build. A clean coordinator build publishes its immutable delta stack for review and proof.
Reopening a file through the proof MCP gives Lean's LSP
one dependency-build pass; resulting cache writes occupy only that agent's writable upper layer and
are discarded at teardown. The generic MCP build remains hidden. Already-running proof agents remain
pinned to their original clean snapshot, while later attempts see coordinator-published artifacts.
Old snapshots are reclaimed when their last agent exits, and source imports preserve mtimes.
Layer stacks are compacted asynchronously after `cache_compaction_layers` entries; compaction scans
only the mutable project/delta layers and never holds the coordinator build queue.

FUSE isolation requires `fuse-overlayfs`, `fusermount3`, `rsync`, and an accessible `/dev/fuse`.
`auto` selects FUSE when these primitives are present and otherwise uses the explicitly visible
`shared` backend; production runs can require isolation with `--isolation fuse-overlay` so missing
support is a hard error. The fallback `shared` backend serializes complete attempts because source
edits cannot otherwise be isolated from coordinator builds. With full-access Codex workers,
overlays prevent ordinary cwd-relative
collisions and still gate imported changes, but they are not a security boundary: a worker can use
absolute paths to reach the canonical repository or other host files. Enable the Codex sandbox when
that boundary must be enforced. Validation remains associated with the completed agent's global
slot, while the independent build queue guarantees that at most one Lake build runs at a time.

## Configuration

No configuration is required for a conventional numbered LastLib Markdown book. If `paf.toml`
exists in the current directory or any ancestor and no target or `--config` is supplied, the nearest
one is loaded automatically. Every command accepts `--project /absolute/path/to/project` (or a path
to its `paf.toml`), so status and agent-control commands can be run from an unrelated directory.
An explicit source target continues to select that source directly; its project is resolved from an
ancestor `paf.toml`, then an ancestor Git checkout. With neither an explicit project nor target nor
discoverable configuration, the current directory is the project root.

Project-relative state remains under `.paf/` by default. `state_dir` is resolved relative to
`swarm.repo` and may also be an absolute path outside the project. Checkpoints and new run records
store the resolved project root, allowing project-local state to rebind correctly after the whole
project is moved. Explicit top-level settings override these defaults:

```toml
[swarm]
repo = "."
state_dir = ".paf"
max_agents = 24
codex_bin = "codex"
model = "gpt-5.6-luna"
reasoning_effort = "max"
sandbox = "danger-full-access"
approve_for_me = false
bypass_approvals_and_sandbox = true
agent_timeout_seconds = 7200
capacity_resume_attempts = 10 # resume the same Codex thread after transient capacity failures
capacity_resume_delay_seconds = 15 # initial delay; doubles after each failure
capacity_resume_max_delay_seconds = 120 # cap for exponential backoff
codex_fd_recycle_threshold = 256 # recycle a leaking Codex/MCP process at this many FDs; 0 disables
codex_fd_recycle_attempts = 20 # maximum transparent same-thread resource recycles per agent
validation_timeout_seconds = 1800
isolation = "auto" # auto, fuse-overlay, or shared
cache_compaction_layers = 32 # asynchronously compact immutable cache layer stacks at this size
lean_project = "lean" # relative to swarm.repo; contains lakefile and lean-toolchain
lean_mcp_tool_timeout_seconds = 300
```

Failure repair is opt-in. The Shepherd uses a strong read-only model to plan a bounded repair DAG
every 20 minutes or whenever 10 new terminal failures accumulate; scoped editing remains on
Luna/max workers and shares the ordinary stage locks and capacity:

```toml
[shepherd]
enabled = true
model = "gpt-5.6-sol"
reasoning_effort = "medium"
worker_model = "gpt-5.6-luna"
worker_reasoning_effort = "max"
interval_seconds = 1200
failure_threshold = 10
maximum_failures_per_sweep = 50
maximum_work_units_per_sweep = 32
maximum_sweeps_per_invocation = 3
max_agents = 2
```

Repair is an auxiliary overlay, not a fifth stage. The Shepherd may target any existing
discover/formalize/review/prove cell. While the Luna worker runs, the TUI and web matrix label that
cell `repairing`; when the coordinator build validates it, the cell switches to `building`. The
underlying task status changes only after validation succeeds.

When Lean MCP is enabled, orchestrator startup bootstraps `lean_project` if it does not already
contain both `lean-toolchain` and a Lake file. PAF pins the active Lean version, creates a matching
Mathlib dependency and Lean library, and runs `lake update` before starting any agents. Existing
valid Lake projects are left unchanged. An interrupted bootstrap is retried on the next startup.

Every stage automatically uses its packaged standard prompt and default retry bound. It inherits
the swarm model and reasoning effort unless the stage has an override; discovery defaults to
`gpt-5.6-luna` at `medium`. A stage table may override any of these settings independently:

```toml
[stages.discover]
max_agents = 40 # discovery-only pool; other stages share swarm.max_agents
model = "gpt-5.6-luna"
reasoning_effort = "medium"

[stages.formalize]
model = "gpt-5.6-sol"
reasoning_effort = "high"
max_rounds = 12

[stages.review]
prompt = "my-prompts/strict-review.md"
max_rounds = 6
```

Books are repeatable. Chapters omitted from `chapters` are discovered from every matching Markdown
heading. Template expansion creates module names, validation commands, and exclusive scopes.

```toml
[[books]]
id = "book03"
title = "Ramification Theory"
source = "books/03-ramification-theory.md"
lean_root = "lean/LastLib/Book03RamificationTheory"
module = "LastLib.Book03RamificationTheory"
depends_on = ["book01", "book02"]
statement_effort = 18.5
proof_effort = 42
chapters = [1, 2, 3]
chapter_path = "Chapter{chapter_number_padded}"
chapter_module = "{module}.Chapter{chapter_number_padded}"
build_command = "cd lean && lake build +{chapter_module}"
scope = [
  "{lean_root}/{chapter_path}.lean",
  "{lean_root}/{chapter_path}/**/*.lean",
]
```

Every `depends_on` id must appear in the same configuration. When a CLI selection excludes an
otherwise configured prerequisite, that prerequisite is treated as pre-existing and satisfied.

As an alternative to explicit `[[books]]` entries, `[sources]` can discover a mixed-format corpus.
Paths are normalized relative to `swarm.repo`, de-duplicated, and sorted before the optional
manifest order is applied. Include and exclude entries are evaluated in order; prefix a later
exclude with `!` to restore a path.

```toml
[sources]
roots = ["notes", "appendices/overview.txt"]
include = ["**/*.md", "**/*.tex", "**/*.txt"]
exclude = ["**/drafts/**", "**/generated/**", "!notes/drafts/released.md"]
manifest = ["notes/introduction.md", "appendices/overview.txt"]

[sources.dependencies]
"appendices/overview.txt" = ["notes/introduction.md"]

[[sources.rules]]
glob = "lecture-notes/**/*.tex"
format = "latex"
unit = "section"
follow_includes = true

[[sources.rules]]
glob = "appendices/*.txt"
format = "text"
heading_pattern = "^CHAPTER (?P<number>\\d+): (?P<title>.+)$"
```

The manifest may be a list as above or a path to a newline-delimited (or JSON-list) manifest.
Listed documents come first; discovered documents omitted from a partial manifest retain their
stable repository-relative order afterward.

When a project already records document order inside another source file, a manifest extraction
table can derive paths without duplicating that order in PAF configuration. The regular expression
is scanned in file order, and `template` formats its named capture groups into repository-relative
document paths:

```toml
[sources]
roots = ["books"]

[sources.manifest]
path = "table-of-contents.tex"
pattern = '\\hyperref\[(?P<name>.+?)-section-phantom\]'
template = "books/{name}.tex"
allow_missing = true
```

By default, every extracted path must be among the discovered documents. Set `allow_missing` only
when the ordering source intentionally mentions entries outside the selected roots (for example,
an index). At least one extracted path must still resolve. Configured source order is also the
display order and the deterministic tie-breaker when dependency-ready documents have equal
critical-path priority.

## Development checks

Run all required tooling through uv:

```console
uv run ruff format --check .
uv run ruff check .
uv run ty check
uv run pytest
```
