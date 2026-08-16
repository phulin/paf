# PAF Observatory

A Vite/React companion to the Textual TUI. The installed Python service owns all project filesystem
and state access; Vite only builds the frontend and proxies `/api` to that same service in
development.

```console
# terminal 1, from the repository root
uv run paf web . --port 8000

# terminal 2
cd web
npm install
npm run dev
```

Open `http://127.0.0.1:5173`. Set `PAF_API_URL` before `npm run dev` when the Python API is not at
`http://127.0.0.1:8000`. The header menu lists durable runs from the configured state directory,
puts currently running swarms first, and remembers the selected run. The overview refreshes that
swarm every three seconds. The statement browser indexes Lean declarations under the configured
target root. The status bar also reports live host CPU utilization and used/total RAM.

For normal installed use, run `paf web /path/to/project`; no Node process is involved.

Other commands:

```console
npm run release:bundle
npm run check:bundle
npm run preview
```

`release:bundle` runs the production build into `src/paf/web_dist/`, records SHA-256 digests for
all frontend sources, TypeScript/Vite configuration, npm metadata, and generated files, then checks
the result. That output directory is ignored and must not be committed. `check:bundle` is a
deterministic, Node-free freshness check suitable after a local or CI build. It fails when an input
is added, removed, or changed, or when the generated bundle is missing, edited, or contains an
unhashed asset.

Node and npm are contributor and release-build requirements. The Python wheel and sdist contain the
already built UI and do not run npm during installation.
