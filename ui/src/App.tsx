// App.tsx
import React, { useEffect, useMemo, useRef, useState } from "react";

const API_DEFAULT = "http://127.0.0.1:8000";

/* ===== Tipi ===== */
type PlaceRow = {
  place_id: string;
  name: string;
  city?: string | null;
  category?: string | null;
  total_score: number;
  popularity_score: number;
  territory_score: number;
  accessibility_score: number;
};

type CountRow = { tbl: string; count: number };

type Filters = {
  city: string;
  category: string;
  min_score: number;
  limit: number;
};

type JobState = {
  status: "idle" | "running" | "ok" | "error";
  started_at?: string | null;
  ended_at?: string | null;
  last_rc?: number | null;
  last_lines?: string[];
};

type ETLStatus = {
  overpass: JobState;
  pipeline: JobState;
};

/* ===== Componenti ===== */
const ScoreBadge: React.FC<{ value: number }> = ({ value }) => {
  const color =
    value >= 80 ? "bg-emerald-600" :
    value >= 60 ? "bg-green-500" :
    value >= 40 ? "bg-amber-500" :
    "bg-rose-600";
  return (
    <span className={`inline-flex items-center px-2.5 py-1.5 text-xs font-semibold text-white rounded-full ${color}`}>
      {Math.round(value)}
    </span>
  );
};

const Row: React.FC<{ r: PlaceRow }> = ({ r }) => (
  <tr className="border-b hover:bg-slate-50">
    <td className="py-3.5 px-4 font-medium">{r.name}</td>
    <td className="py-3.5 px-4 text-slate-600">{r.city ?? "—"}</td>
    <td className="py-3.5 px-4 text-slate-600">{r.category ?? "—"}</td>
    <td className="py-3.5 px-4"><ScoreBadge value={r.total_score || 0} /></td>
    <td className="py-3.5 px-4">
      <div className="flex gap-1.5 text-xs text-slate-600">
        <span title="Popularity (0–50)" className="px-2 py-1 rounded bg-slate-100">⭐ {Math.round(r.popularity_score || 0)}</span>
        <span title="Context (0–30)" className="px-2 py-1 rounded bg-slate-100">🌍 {Math.round(r.territory_score || 0)}</span>
        <span title="Accessibility (0–20)" className="px-2 py-1 rounded bg-slate-100">🕓 {Math.round(r.accessibility_score || 0)}</span>
      </div>
    </td>
  </tr>
);

export default function App() {
  const [apiBase, setApiBase] = useState<string>(API_DEFAULT);
  const [filters, setFilters] = useState<Filters>({ city: "", category: "", min_score: 0, limit: 50 });
  const [rows, setRows] = useState<PlaceRow[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string>("");
  const [counts, setCounts] = useState<CountRow[] | null>(null);
  const [healthy, setHealthy] = useState<boolean>(false);
  const [etl, setEtl] = useState<ETLStatus | null>(null);
  const pollRef = useRef<number | null>(null);

  const qs = useMemo(() => {
    const q = new URLSearchParams();
    if (filters.city) q.set("city", filters.city);
    if (filters.category) q.set("category", filters.category);
    q.set("min_score", String(filters.min_score ?? 0));
    q.set("limit", String(filters.limit ?? 50));
    return q.toString();
  }, [filters]);

  async function fetchCounts() {
    try {
      const r = await fetch(`${apiBase}/counts`);
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const data = (await r.json()) as CountRow[];
      setCounts(data);
    } catch {
      setCounts(null);
    }
  }

  async function fetchHealth() {
    try {
      const r = await fetch(`${apiBase}/health`);
      if (!r.ok) throw new Error();
      await r.json();
      setHealthy(true);
    } catch {
      setHealthy(false);
    }
  }

  async function fetchEtlStatus() {
    try {
      const r = await fetch(`${apiBase}/etl/status`);
      if (!r.ok) throw new Error();
      const data = (await r.json()) as ETLStatus;
      setEtl(data);
      // quando termina un job, aggiorna i counts
      if (
        data.pipeline?.status === "ok" ||
        data.overpass?.status === "ok"
      ) {
        fetchCounts();
      }
    } catch {
      setEtl(null);
    }
  }

  async function startOverpass() {
    try {
      const r = await fetch(`${apiBase}/etl/overpass/start`, { method: "POST" });
      if (!r.ok) throw new Error(await r.text());
      await fetchEtlStatus();
      startPolling();
    } catch (e) {
      alert(`Overpass: ${String(e)}`);
    }
  }

  async function startPipeline() {
    try {
      const r = await fetch(`${apiBase}/etl/pipeline/start`, { method: "POST" });
      if (!r.ok) throw new Error(await r.text());
      await fetchEtlStatus();
      startPolling();
    } catch (e) {
      alert(`Pipeline: ${String(e)}`);
    }
  }

  function startPolling() {
    if (pollRef.current) return;
    pollRef.current = window.setInterval(() => {
      fetchEtlStatus();
    }, 3000);
  }

  function stopPolling() {
    if (pollRef.current) {
      clearInterval(pollRef.current);
      pollRef.current = null;
    }
  }

  async function search() {
    setLoading(true); setError("");
    try {
      const r = await fetch(`${apiBase}/places?${qs}`);
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const data = (await r.json()) as PlaceRow[];
      setRows(data);
    } catch (e) {
      setError(String(e));
      setRows([]);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    fetchHealth();
    fetchCounts();
    fetchEtlStatus();
  }, [apiBase]);

  // ferma il polling se nessun job è running
  useEffect(() => {
    const anyRunning = etl?.overpass?.status === "running" || etl?.pipeline?.status === "running";
    if (anyRunning) startPolling(); else stopPolling();
    return () => stopPolling();
  }, [etl]);

  return (
    <div className="min-h-screen bg-white text-slate-900">
      <div className="max-w-6xl mx-auto p-4 md:p-6 lg:p-8">
        <header className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3 mb-6 md:mb-8">
          <div>
            <h1 className="text-2xl font-bold">CustumerTarget – Mini GUI</h1>
            <p className="text-sm text-slate-600">Query FastAPI e visualizza gli score.</p>
          </div>
          <span className={`inline-flex items-center gap-1 text-xs px-2.5 py-1.5 rounded-full ${healthy ? "bg-emerald-100 text-emerald-700" : "bg-rose-100 text-rose-700"}`}>
            <span className={`w-2 h-2 rounded-full ${healthy ? "bg-emerald-500" : "bg-rose-500"}`} />
            {healthy ? "API online" : "API offline"}
          </span>
        </header>

        <section className="mb-6 md:mb-8">
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3 md:gap-4">
            <label className="flex flex-col text-sm">
              <span className="mb-1.5 md:mb-2">API base URL</span>
              <input
                className="ui-input"
                value={apiBase}
                onChange={e => setApiBase(e.target.value)}
              />
            </label>
            <label className="flex flex-col text-sm">
              <span className="mb-1.5 md:mb-2">City (optional)</span>
              <input
                className="border rounded px-3 py-2.5 focus:outline-none focus:ring-2 focus:ring-blue-200"
                placeholder="Alatri"
                value={filters.city}
                onChange={e => setFilters({ ...filters, city: e.target.value })}
              />
            </label>
            <label className="flex flex-col text-sm">
              <span className="mb-1.5 md:mb-2">Category (optional)</span>
              <input
                className="border rounded px-3 py-2.5 focus:outline-none focus:ring-2 focus:ring-blue-200"
                placeholder="restaurant"
                value={filters.category}
                onChange={e => setFilters({ ...filters, category: e.target.value })}
              />
            </label>
            <label className="flex flex-col text-sm">
              <span className="mb-1.5 md:mb-2">Min score</span>
              <input
                type="number"
                className="border rounded px-3 py-2.5 focus:outline-none focus:ring-2 focus:ring-blue-200"
                min={0}
                max={100}
                value={filters.min_score}
                onChange={e => setFilters({ ...filters, min_score: Number(e.target.value) })}
              />
            </label>
            <label className="flex flex-col text-sm">
              <span className="mb-1.5 md:mb-2">Limit</span>
              <input
                type="number"
                className="border rounded px-3 py-2.5 focus:outline-none focus:ring-2 focus:ring-blue-200"
                min={1}
                max={200}
                value={filters.limit}
                onChange={e => setFilters({ ...filters, limit: Number(e.target.value) })}
              />
            </label>
          </div>

          <div className="mt-4 flex flex-wrap gap-3">
            <button
              onClick={search}
              className="ui-btn ui-btn-ghost"
              disabled={loading}
            >
              Search
            </button>
            <button
              onClick={() => { setFilters({ city: "", category: "", min_score: 0, limit: 50 }); setRows([]); }}
              className="ui-btn ui-btn-ghost"            >
              Reset
            </button>
            <span className="mx-2 w-px bg-slate-200" />
            <button
              onClick={startOverpass}
              className="ui-btn ui-btn-ghost"
              disabled={etl?.overpass?.status === "running"}
              title="Esegue l'estrazione da Overpass (OSM)"
            >
              Run Overpass
            </button>
            <button
              onClick={startPipeline}
              className="ui-btn ui-btn-ghost"
              disabled={etl?.pipeline?.status === "running"}
              title="Esegue gli step SQL della pipeline"
            >
              Run Pipeline
            </button>
            <button
              onClick={() => fetchEtlStatus()}
              className="ui-btn ui-btn-ghost"
            >
              Refresh Status
            </button>
          </div>
        </section>

        {etl && (
          <section className="mb-6">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {(["overpass", "pipeline"] as const).map((k) => (
                <div key={k} className="border rounded-xl p-3">
                  <div className="flex items-center justify-between mb-2">
                    <h3 className="font-semibold capitalize">{k}</h3>
                    <span className={`text-xs px-2 py-1 rounded-full ${
                      etl[k].status === "running" ? "bg-amber-100 text-amber-700" :
                      etl[k].status === "ok" ? "bg-emerald-100 text-emerald-700" :
                      etl[k].status === "error" ? "bg-rose-100 text-rose-700" :
                      "bg-slate-100 text-slate-700"
                    }`}>
                      {etl[k].status}
                    </span>
                  </div>
                  <div className="text-xs text-slate-600 mb-2">
                    <div>started: {etl[k].started_at ?? "-"}</div>
                    <div>ended: {etl[k].ended_at ?? "-"}</div>
                    {typeof etl[k].last_rc === "number" && <div>rc: {etl[k].last_rc}</div>}
                  </div>
                  {etl[k].last_lines && etl[k].last_lines.length > 0 && (
                    <pre className="text-xs bg-slate-50 border rounded p-2 max-h-48 overflow-auto">
                      {etl[k].last_lines.slice(-20).join("\n")}
                    </pre>
                  )}
                </div>
              ))}
            </div>
          </section>
        )}

        {counts && (
          <section className="mb-6">
            <div className="text-sm text-slate-600 flex flex-wrap gap-2 md:gap-3">
              {counts.map((c, i) => (
                <span key={i} className="px-2.5 py-1.5 rounded bg-slate-100">
                  {c.tbl}: <b>{c.count}</b>
                </span>
              ))}
            </div>
          </section>
        )}

        <section className="bg-white rounded-2xl shadow border overflow-hidden">
          <div className="overflow-x-auto">
            <table className="table.ui-table thead.ui-thead th.ui-th td.ui-td min-w-full text-sm">
              <thead className="bg-slate-50 border-b">
                <tr>
                  <th className="text-left py-3 px-4">Name</th>
                  <th className="text-left py-3 px-4">City</th>
                  <th className="text-left py-3 px-4">Category</th>
                  <th className="text-left py-3 px-4">Score</th>
                  <th className="text-left py-3 px-4">Breakdown</th>
                </tr>
              </thead>
              <tbody>
                {loading ? (
                  <tr><td colSpan={5} className="py-12 px-4 text-center text-slate-500">Loading…</td></tr>
                ) : rows.length === 0 ? (
                  <tr><td colSpan={5} className="py-12 px-4 text-center text-slate-500">No results yet. Adjust filters and press Search.</td></tr>
                ) : (
                  rows.map((r) => <Row key={r.place_id} r={r} />)
                )}
              </tbody>
            </table>
          </div>
        </section>

        {error && <p className="mt-3 text-rose-600 text-sm">{error}</p>}
      </div>
    </div>
  );
}
