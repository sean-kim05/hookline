# Hookline dashboard

A Next.js operator dashboard for Hookline: watch recent deliveries, inspect the
dead-letter queue and replay from it, and register endpoints.

The browser never sees the API key. Every call goes to a server-side proxy
(`app/api/hookline/[...path]`) that attaches the bearer token from
`HOOKLINE_API_KEY` and forwards to `HOOKLINE_URL`.

## Develop

```bash
cd dashboard
npm install
cp .env.example .env.local   # point at a running hookline
npm run dev                  # http://localhost:3001
```

Run Hookline with the read APIs enabled (any backend exposes them):

```bash
go run ./cmd/hookline -api-key dev-key
```

## End-to-end tests

```bash
npm run test:e2e
```

Playwright starts the dev server and intercepts the proxy calls with fixture
data, so the UI (delivery filtering, DLQ replay, endpoint registration) is
tested deterministically without a live backend.
