import { NextRequest, NextResponse } from "next/server";

// HTTP Basic Auth gate for the whole dashboard (pages + the API proxy).
//
// Auth is enabled only when DASHBOARD_PASSWORD is set, so local development
// stays frictionless. In a public deployment, set DASHBOARD_USER (default
// "admin") and DASHBOARD_PASSWORD and the browser will prompt for them.
export function middleware(req: NextRequest) {
  const password = process.env.DASHBOARD_PASSWORD;
  if (!password) {
    return NextResponse.next(); // auth disabled (no password configured)
  }
  const user = process.env.DASHBOARD_USER || "admin";

  const header = req.headers.get("authorization") || "";
  if (header.startsWith("Basic ")) {
    const decoded = atob(header.slice("Basic ".length));
    const sep = decoded.indexOf(":");
    const u = decoded.slice(0, sep);
    const p = decoded.slice(sep + 1);
    if (u === user && p === password) {
      return NextResponse.next();
    }
  }

  return new NextResponse("Authentication required.", {
    status: 401,
    headers: { "WWW-Authenticate": 'Basic realm="Hookline", charset="UTF-8"' },
  });
}

// Protect everything except Next.js's own static assets.
export const config = {
  matcher: ["/((?!_next/static|_next/image|favicon.ico).*)"],
};
