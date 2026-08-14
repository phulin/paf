# LastLib Observatory

A Vite/React companion to the Textual TUI. It polls the newest durable swarm state and indexes Lean
declarations directly from the repository while the development server is running.

```console
cd web
npm install
npm run dev
```

Open `http://localhost:5173`. The header menu discovers every `.swarm/*/state.json`, puts currently
running swarms first, and remembers the selected run. The overview refreshes that swarm every three
seconds. The statement browser indexes `lean/LastLib/**/*.lean`, caches unchanged files, and
refreshes edited files on subsequent searches. A bundled demo snapshot keeps the production bundle
usable when it is served without repository filesystem access.

Other commands:

```console
npm run build
npm run preview
```
