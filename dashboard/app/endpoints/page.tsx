"use client";

import { useEffect, useState, useCallback } from "react";
import { api, Endpoint } from "@/lib/api";

export default function EndpointsPage() {
  const [rows, setRows] = useState<Endpoint[]>([]);
  const [url, setUrl] = useState("");
  const [error, setError] = useState("");
  const [secret, setSecret] = useState("");
  const [busy, setBusy] = useState(false);

  const load = useCallback(async () => {
    try {
      const { endpoints } = await api.endpoints();
      setRows(endpoints ?? []);
      setError("");
    } catch (e) {
      setError(String(e));
    }
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  async function register(e: React.FormEvent) {
    e.preventDefault();
    if (!url.trim()) return;
    setBusy(true);
    setError("");
    setSecret("");
    try {
      const created = await api.registerEndpoint(url.trim());
      setSecret(created.secret);
      setUrl("");
      await load();
    } catch (err) {
      setError(String(err));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div>
      <h1>Endpoints</h1>
      <p className="sub">Registered delivery destinations. Each has its own signing secret.</p>

      <form className="row" onSubmit={register}>
        <input
          placeholder="https://consumer.example.com/webhooks"
          value={url}
          onChange={(e) => setUrl(e.target.value)}
          data-testid="endpoint-url"
        />
        <button type="submit" disabled={busy} data-testid="register">
          {busy ? "Registering…" : "Register"}
        </button>
      </form>

      {error && <div className="error">{error}</div>}
      {secret && (
        <div className="notice" data-testid="secret-notice">
          Signing secret (shown once — copy it now):
          <div className="secret">{secret}</div>
        </div>
      )}

      {rows.length === 0 ? (
        <div className="empty">No endpoints registered yet.</div>
      ) : (
        <table data-testid="endpoints-table">
          <thead>
            <tr>
              <th>URL</th>
              <th>Producer</th>
              <th>Status</th>
              <th>Registered</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((ep) => (
              <tr key={ep.id}>
                <td className="mono">{ep.url}</td>
                <td>{ep.producer || "—"}</td>
                <td>{ep.disabled ? "disabled" : "active"}</td>
                <td className="mono">{new Date(ep.created_at).toLocaleString()}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
