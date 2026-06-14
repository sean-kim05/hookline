// Client-side helpers that call the server proxy at /api/hookline/*.

export type Delivery = {
  id: string;
  message_id: string;
  event_id: string;
  endpoint: string;
  attempt: number;
  outcome: "delivered" | "retrying" | "dead_lettered";
  status_code: number;
  duration_ms: number;
  error?: string;
  at: string;
};

export type DeadLetter = {
  message_id: string;
  event_id: string;
  endpoint: string;
  attempts: number;
  reason: string;
  payload: unknown;
  failed_at: string;
};

export type Endpoint = {
  id: string;
  url: string;
  producer?: string;
  disabled: boolean;
  created_at: string;
};

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`/api/hookline/${path}`, { cache: "no-store" });
  if (!res.ok) throw new Error(`request failed: ${res.status}`);
  return res.json() as Promise<T>;
}

async function post<T>(path: string, body?: unknown): Promise<T> {
  const res = await fetch(`/api/hookline/${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (!res.ok) {
    const msg = await res.text();
    throw new Error(`request failed: ${res.status} ${msg}`);
  }
  return res.json() as Promise<T>;
}

export const api = {
  deliveries: (outcome?: string) =>
    get<{ deliveries: Delivery[] }>(`v1/deliveries${outcome ? `?outcome=${outcome}` : ""}`),
  dlq: () => get<{ dead_letters: DeadLetter[] }>("v1/dlq"),
  replay: (messageID: string) => post<{ id: string; event_id: string }>(`v1/dlq/${messageID}/replay`),
  endpoints: () => get<{ endpoints: Endpoint[] }>("v1/endpoints"),
  registerEndpoint: (url: string) =>
    post<Endpoint & { secret: string }>("v1/endpoints", { url }),
};
