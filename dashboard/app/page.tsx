"use client";

import { useEffect, useState, useCallback } from "react";
import { api, Delivery } from "@/lib/api";

const FILTERS = ["", "delivered", "retrying", "dead_lettered"] as const;

export default function DeliveriesPage() {
  const [rows, setRows] = useState<Delivery[]>([]);
  const [outcome, setOutcome] = useState<string>("");
  const [error, setError] = useState<string>("");

  const load = useCallback(async () => {
    try {
      const { deliveries } = await api.deliveries(outcome);
      setRows(deliveries ?? []);
      setError("");
    } catch (e) {
      setError(String(e));
    }
  }, [outcome]);

  useEffect(() => {
    load();
    const t = setInterval(load, 3000);
    return () => clearInterval(t);
  }, [load]);

  return (
    <div>
      <h1>Deliveries</h1>
      <p className="sub">Recent delivery attempts, refreshed every 3s.</p>

      <div className="row">
        {FILTERS.map((f) => (
          <button
            key={f || "all"}
            className={outcome === f ? "" : "secondary"}
            onClick={() => setOutcome(f)}
            data-testid={`filter-${f || "all"}`}
          >
            {f === "" ? "All" : f.replace("_", " ")}
          </button>
        ))}
      </div>

      {error && <div className="error">{error}</div>}

      {rows.length === 0 ? (
        <div className="empty">No deliveries yet.</div>
      ) : (
        <table data-testid="deliveries-table">
          <thead>
            <tr>
              <th>Outcome</th>
              <th>Event</th>
              <th>Endpoint</th>
              <th>Attempt</th>
              <th>Status</th>
              <th>Latency</th>
              <th>When</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((d) => (
              <tr key={d.id}>
                <td><span className={`badge ${d.outcome}`}>{d.outcome.replace("_", " ")}</span></td>
                <td className="mono">{d.event_id.slice(0, 12)}</td>
                <td className="mono">{d.endpoint}</td>
                <td>{d.attempt}</td>
                <td>{d.status_code || "—"}</td>
                <td>{d.duration_ms} ms</td>
                <td className="mono">{new Date(d.at).toLocaleTimeString()}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
