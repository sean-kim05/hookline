"use client";

import { useEffect, useState, useCallback } from "react";
import { api, DeadLetter } from "@/lib/api";

export default function DLQPage() {
  const [rows, setRows] = useState<DeadLetter[]>([]);
  const [error, setError] = useState<string>("");
  const [notice, setNotice] = useState<string>("");
  const [busy, setBusy] = useState<string>("");

  const load = useCallback(async () => {
    try {
      const { dead_letters } = await api.dlq();
      setRows(dead_letters ?? []);
      setError("");
    } catch (e) {
      setError(String(e));
    }
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  async function replay(id: string) {
    setBusy(id);
    setNotice("");
    try {
      const res = await api.replay(id);
      setNotice(`Replayed event ${res.event_id} as message ${res.id}.`);
      await load();
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy("");
    }
  }

  return (
    <div>
      <h1>Dead-letter queue</h1>
      <p className="sub">Events that exhausted their retries. Replay re-enqueues the original event.</p>

      {error && <div className="error">{error}</div>}
      {notice && <div className="notice" data-testid="replay-notice">{notice}</div>}

      {rows.length === 0 ? (
        <div className="empty" data-testid="dlq-empty">The dead-letter queue is empty. 🎉</div>
      ) : (
        <table data-testid="dlq-table">
          <thead>
            <tr>
              <th>Event</th>
              <th>Endpoint</th>
              <th>Attempts</th>
              <th>Reason</th>
              <th>Failed</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {rows.map((d) => (
              <tr key={d.message_id}>
                <td className="mono">{d.event_id.slice(0, 12)}</td>
                <td className="mono">{d.endpoint}</td>
                <td>{d.attempts}</td>
                <td>{d.reason}</td>
                <td className="mono">{new Date(d.failed_at).toLocaleString()}</td>
                <td>
                  <button
                    onClick={() => replay(d.message_id)}
                    disabled={busy === d.message_id}
                    data-testid={`replay-${d.message_id}`}
                  >
                    {busy === d.message_id ? "Replaying…" : "Replay"}
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
