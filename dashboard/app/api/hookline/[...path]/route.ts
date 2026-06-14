import { NextRequest } from "next/server";

// Server-side proxy to the Hookline API. The browser talks only to this route;
// the API key lives in server env (HOOKLINE_API_KEY) and never reaches the
// client. Every /api/hookline/<path> request is forwarded to
// <HOOKLINE_URL>/<path> with the bearer token attached.

const BASE = process.env.HOOKLINE_URL ?? "http://localhost:8080";
const KEY = process.env.HOOKLINE_API_KEY ?? "dev-key";

export const dynamic = "force-dynamic";

async function forward(req: NextRequest, path: string[]) {
  const target = `${BASE}/${path.join("/")}${req.nextUrl.search}`;
  const headers: Record<string, string> = { Authorization: `Bearer ${KEY}` };
  const init: RequestInit = { method: req.method, headers };
  if (req.method !== "GET" && req.method !== "HEAD") {
    headers["Content-Type"] = "application/json";
    init.body = await req.text();
  }
  try {
    const resp = await fetch(target, init);
    const body = await resp.text();
    return new Response(body, {
      status: resp.status,
      headers: { "Content-Type": "application/json" },
    });
  } catch {
    return new Response(JSON.stringify({ error: "hookline unreachable" }), {
      status: 502,
      headers: { "Content-Type": "application/json" },
    });
  }
}

export function GET(req: NextRequest, ctx: { params: { path: string[] } }) {
  return forward(req, ctx.params.path);
}

export function POST(req: NextRequest, ctx: { params: { path: string[] } }) {
  return forward(req, ctx.params.path);
}
