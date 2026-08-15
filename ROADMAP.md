# PAF portability roadmap

This plan separates two concerns that are currently coupled to the original repository: how PAF
discovers informal mathematics, and how the Python package and web UI find a project at runtime.
The existing Markdown-to-Lean workflow should remain available as a compatibility profile while
these boundaries are introduced.

## 1. Flexible informal-math inputs

### 1.1 Introduce a source model

Add format-neutral `SourceDocument` and `WorkUnit` types. A document records its path, format,
title, and source metadata; a work unit records a stable id, title, ordinal, source span, and any
declared dependencies. Replace orchestration-facing uses of `BookConfig` and `Chapter` with these
types, keeping adapters for the old names until the scheduler and persisted-state migration are
complete.

Acceptance criteria:

- Scheduler, state, TUI, and web payloads consume work-unit ids without assuming books or chapters.
- Stable ids survive moving the project as a whole and do not depend on absolute paths.
- Existing Markdown configurations load with the same unit boundaries and state ids.

### 1.2 Put parsing behind adapters

Define a `SourceAdapter` interface with `supports(path)`, `read_document(path)`, and
`discover_units(document)`. Ship three adapters:

- Markdown (`.md`): ATX headings by default, with the current numbered-chapter pattern available as
  a compatibility preset.
- LaTeX (`.tex`): ignore comments and verbatim-like environments, recognize configurable
  `\part`, `\chapter`, `\section`, and `\subsection` boundaries, and retain line spans. Follow local
  `\input`/`\include` files only when enabled and reject include cycles. No compilation or PDF
  ingestion is part of this phase.
- Plain text (`.txt`): treat a file as one unit by default; optionally split it with a configured
  heading regex or delimiter. This avoids guessing structure that plain text does not encode.

Put adapter selection and splitting rules in `paf.toml`, with per-path overrides for mixed-format
corpora. Parsing tests should cover comments, duplicate headings, empty sections, Unicode titles,
and deterministic ids.

### 1.3 Make directory discovery explicit and recursive

Replace the current direct-children `*.md` scan with a `SourceResolver` that accepts files or
directories, recursively discovers `.md`, `.tex`, and `.txt`, and applies ordered include/exclude
globs. Ignore `.git`, `.paf`, build outputs, hidden directories, and symlinked directories by
default. Sort normalized repository-relative paths before parsing, then apply configured document
dependencies or an optional manifest order.

Recommended configuration shape:

```toml
[sources]
roots = ["notes", "appendices/overview.txt"]
include = ["**/*.md", "**/*.tex", "**/*.txt"]
exclude = ["**/drafts/**", "**/generated/**"]

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

### 1.4 Separate source structure from target layout

Move Lean paths, module names, scope templates, build commands, diagnostics, and MCP setup behind a
target/backend interface. The first backend remains Lean, but source files no longer need to mirror
`lean/<namespace>/BookNN/ChapterNN`. Each work unit receives its target mapping from templates or an
explicit manifest. This boundary lets a nested TeX corpus map to a flat Lean namespace—or vice
versa—without changing the source resolver.

Acceptance criteria:

- `paf plan path/to/source` prints discovered documents, units, dependencies, and target scopes for
  all three formats.
- Mixed `.md`/`.tex`/`.txt` directories can run through scaffold and orchestration.
- Diagnostics and persisted state refer back to source path and line span, not only a chapter
  number.

## 2. Installable CLI with a portable web UI

### 2.1 Make project resolution independent of PAF's checkout

Add one project resolver used by every command. Resolution order should be: explicit `--project` or
target path, nearest ancestor `paf.toml`, then the current directory. Resolve repository, source,
target, and state paths from that project object. Do not derive project paths from `__file__`, the
installed package directory, or the web source tree.

Keep project-local `.paf/` as the default so runs travel with a checkout, but allow
`state_dir` to point elsewhere. Record the resolved project root in run metadata so the web server
can safely locate live state.

### 2.2 Package the frontend

Build the React app during release preparation and place its hashed assets under
`src/paf/web_dist/`. Include that directory and the prompt Markdown files as wheel package data.
Normal installation must not require Node; Node remains a contributor-only dependency for rebuilding
the frontend. Add a release check that fails when `web/src` is newer than the committed/built
bundle.

### 2.3 Serve UI and API from the installed command

Move the filesystem/state API out of `web/vite.config.ts` into Python. Add:

```console
paf web [PROJECT] --host 127.0.0.1 --port 5173
```

The command should serve packaged static assets plus project-scoped JSON endpoints for runs,
snapshots, host load, and source/target browsing. Validate every requested path against the resolved
project roots, bind to loopback by default, and make `--host 0.0.0.0` an explicit choice. Vite's
development server should proxy to the same Python API so development and installed behavior share
one backend.

### 2.4 Complete packaging and outside-directory tests

Keep `paf = "paf.cli:main"` as the console entry point and document `uv tool install .` (plus a
published-package form once a distribution name is chosen). Add wheel/sdist builds and test them in
a clean temporary environment:

- Run `paf --version` from a directory unrelated to this checkout.
- Run `paf plan /absolute/path/to/project` for `.md`, `.tex`, `.txt`, and a mixed directory.
- Start `paf web /absolute/path/to/project` outside the project and smoke-test the HTML and JSON
  endpoints.
- Verify prompts and frontend assets come from `importlib.resources` inside the installed wheel.
- Verify a project-local `paf.toml` and an external `state_dir` both work.

## Suggested delivery order

1. Source model and compatibility adapters.
2. Markdown, LaTeX, and text adapters plus recursive resolution.
3. Target/backend boundary and persisted-state migration.
4. Shared project resolver and `paf web` Python API.
5. Bundled frontend, wheel tests, and installation documentation.

Each step should land with compatibility tests so existing orchestration behavior remains usable
while the old LastLib-specific inference rules are retired.
