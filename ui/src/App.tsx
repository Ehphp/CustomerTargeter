// App.tsx
import React, { useEffect, useMemo, useRef, useState } from "react";

const API_DEFAULT = "http://127.0.0.1:8000";

/* ===== Tipi ===== */
type PlaceRow = {
  place_id: string;
  name: string;
  city?: string | null;
  category?: string | null;
  sector_density_score?: number | null;
  sector_density_neighbors?: number | null;
  geo_distribution_label?: string | null;
  geo_distribution_source?: string | null;
  size_class?: string | null;
  is_chain?: boolean | null;
  ad_budget_band?: string | null;
  umbrella_affinity?: number | null;
  digital_presence?: number | null;
  digital_presence_confidence?: number | null;
  marketing_attitude?: number | null;
  facts_confidence?: number | null;
  facts_confidence_override?: number | null;
  metrics_updated_at?: string | null;
  website_url?: string | null;
  social?: Record<string, string> | null;
  facts_marketing_attitude?: number | null;
  facts_umbrella_affinity?: number | null;
  budget_source?: string | null;
  provenance?: Record<string, unknown> | null;
  notes?: string | null;
  facts_updated_at?: string | null;
  source_provider?: string | null;
  source_model?: string | null;
};

type SortColumn =
  | "name"
  | "city"
  | "category"
  | "umbrella_affinity"
  | "digital_presence"
  | "sector_density_score";

type SortState = {
  column: SortColumn;
  direction: "asc" | "desc";
};

type CountRow = { tbl: string; count: number };

type ChainFilter = "any" | "yes" | "no";

type Filters = {
  city: string;
  category: string;
  geo_label: string;
  size_class: string;
  ad_budget: string;
  is_chain: ChainFilter;
  min_affinity: number;
  min_density: number;
  min_digital: number;
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

const DEFAULT_SORT_DIRECTION: Record<SortColumn, SortState["direction"]> = {
  name: "asc",
  city: "asc",
  category: "asc",
  umbrella_affinity: "desc",
  digital_presence: "desc",
  sector_density_score: "desc",
};

function compareStrings(
  a: string | null | undefined,
  b: string | null | undefined,
  direction: SortState["direction"],
) {
  const aVal = (a ?? "").trim().toLowerCase();
  const bVal = (b ?? "").trim().toLowerCase();
  const aEmpty = aVal.length === 0;
  const bEmpty = bVal.length === 0;

  if (aEmpty || bEmpty) {
    if (aEmpty === bEmpty) return 0;
    return aEmpty ? 1 : -1;
  }

  return direction === "asc"
    ? aVal.localeCompare(bVal)
    : bVal.localeCompare(aVal);
}

function compareNumbers(
  a: number | null | undefined,
  b: number | null | undefined,
  direction: SortState["direction"],
) {
  const aMissing = a == null || Number.isNaN(a);
  const bMissing = b == null || Number.isNaN(b);

  if (aMissing || bMissing) {
    if (aMissing === bMissing) return 0;
    return aMissing ? 1 : -1;
  }

  if (a === b) return 0;
  return direction === "asc"
    ? a < b ? -1 : 1
    : a < b ? 1 : -1;
}

function compareByColumn(
  a: PlaceRow,
  b: PlaceRow,
  column: SortColumn,
  direction: SortState["direction"],
) {
  switch (column) {
    case "name":
      return compareStrings(a.name, b.name, direction);
    case "city":
      return compareStrings(a.city, b.city, direction);
    case "category":
      return compareStrings(a.category, b.category, direction);
    case "umbrella_affinity":
      return compareNumbers(a.umbrella_affinity, b.umbrella_affinity, direction);
    case "digital_presence":
      return compareNumbers(a.digital_presence, b.digital_presence, direction);
    case "sector_density_score":
      return compareNumbers(a.sector_density_score, b.sector_density_score, direction);
    default:
      return 0;
  }
}

function clamp01(value: number | null | undefined): number | null {
  if (value == null || Number.isNaN(value)) return null;
  return Math.min(1, Math.max(0, value));
}

function formatPercent(value: number | null | undefined): string {
  const pct = clamp01(value);
  if (pct == null) return "-";
  return `${Math.round(pct * 100)}%`;
}

function humanizeLabel(label: string | null | undefined): string {
  if (!label) return "-";
  const normalized = label.replace(/_/g, " ").trim();
  if (normalized.toLowerCase() === "vicino brello") return "Vicino Brellò";
  if (normalized.toLowerCase() === "passaggio") return "Passaggio";
  if (normalized.toLowerCase() === "centro") return "Centro storico";
  return normalized.charAt(0).toUpperCase() + normalized.slice(1);
}

function sanitizeId(value: string): string {
  return value.replace(/[^A-Za-z0-9_-]/g, "_");
}

function formatDate(value: string | null | undefined): string {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString("it-IT", { dateStyle: "short", timeStyle: "short" });
}

const PercentBadge: React.FC<{ value?: number | null }> = ({ value }) => {
  const pct = clamp01(value);
  if (pct == null) {
    return <span className="inline-flex items-center justify-center px-2 py-1 text-xs rounded-full bg-slate-100 text-slate-500">-</span>;
  }
  const perc = pct * 100;
  const color =
    perc >= 75 ? "bg-emerald-100 text-emerald-700" :
    perc >= 50 ? "bg-amber-100 text-amber-700" :
    perc >= 25 ? "bg-sky-100 text-sky-700" :
    "bg-slate-200 text-slate-600";
  return (
    <span className={`inline-flex items-center justify-center px-2 py-1 text-xs font-semibold rounded-full ${color}`}>
      {Math.round(perc)}%
    </span>
  );
};

const ConfidencePill: React.FC<{ value?: number | null }> = ({ value }) => {
  const pct = clamp01(value);
  if (pct == null) return null;
  const perc = Math.round(pct * 100);
  return (
    <span className="inline-flex items-center px-2 py-0.5 rounded-full bg-blue-50 text-blue-700 text-xs font-medium">
      conf {perc}%
    </span>
  );
};

type DetailEntry = { label: string; value: React.ReactNode };

const DetailList: React.FC<{ entries: DetailEntry[] }> = ({ entries }) => (
  <dl className="space-y-1.5">
    {entries.map((entry, idx) => (
      <div key={`${entry.label}-${idx}`} className="flex gap-3">
        <dt className="w-36 shrink-0 text-xs uppercase tracking-wide text-slate-500">{entry.label}</dt>
        <dd className="text-sm text-slate-700 break-words">{entry.value ?? "-"}</dd>
      </div>
    ))}
  </dl>
);

type RowProps = {
  r: PlaceRow;
  expanded: boolean;
  onToggle: () => void;
};

const Row: React.FC<RowProps> = ({ r, expanded, onToggle }) => {
  const confidence = r.facts_confidence_override ?? r.facts_confidence ?? r.digital_presence_confidence;
  const detailId = `details-${sanitizeId(r.place_id)}`;
  return (
    <>
      <tr className="border-b hover:bg-slate-50 transition">
        <td className="py-3.5 px-4 align-top">
          <div className="flex gap-3">
            <button
              type="button"
              onClick={onToggle}
              aria-expanded={expanded}
              aria-controls={detailId}
              className="mt-0.5 inline-flex h-6 w-6 items-center justify-center rounded-full border border-slate-200 text-slate-500 hover:bg-slate-100 focus:outline-none focus:ring-2 focus:ring-blue-400"
            >
              <span aria-hidden="true">{expanded ? "v" : ">"}</span>
              <span className="sr-only">Mostra dettagli</span>
            </button>
            <div>
              <div className="font-medium text-slate-900">{r.name}</div>
              <div className="mt-1 flex flex-wrap items-center gap-2 text-xs text-slate-500">
                {r.size_class && (
                  <span className="px-2 py-0.5 rounded-full bg-slate-100 uppercase tracking-wide">
                    {r.size_class}
                  </span>
                )}
                {r.ad_budget_band && (
                  <span className="px-2 py-0.5 rounded-full bg-amber-100 text-amber-700 uppercase tracking-wide">
                    budget {r.ad_budget_band}
                  </span>
                )}
                {typeof r.is_chain === "boolean" && (
                  <span className="px-2 py-0.5 rounded-full bg-indigo-100 text-indigo-700">
                    {r.is_chain ? "Catena" : "Indipendente"}
                  </span>
                )}
                {confidence != null && <ConfidencePill value={confidence} />}
              </div>
            </div>
          </div>
        </td>
        <td className="py-3.5 px-4 text-slate-600">{r.city ?? "-"}</td>
        <td className="py-3.5 px-4 text-slate-600">{r.category ?? "-"}</td>
        <td className="py-3.5 px-4 text-slate-600">{humanizeLabel(r.geo_distribution_label)}</td>
        <td className="py-3.5 px-4 text-right">
          <PercentBadge value={r.umbrella_affinity} />
        </td>
        <td className="py-3.5 px-4 text-right">
          <PercentBadge value={r.digital_presence} />
        </td>
        <td className="py-3.5 px-4 text-right">
          <div className="flex items-center justify-end gap-2">
            <PercentBadge value={r.sector_density_score} />
            <span className="text-xs text-slate-500 whitespace-nowrap">
              {r.sector_density_neighbors ?? 0} vicini
            </span>
          </div>
        </td>
      </tr>
      {expanded && (
        <tr className="border-b bg-slate-50">
          <td colSpan={7} className="px-4 pb-5 pt-0 text-sm text-slate-600">
            <div id={detailId} className="pt-4">
              <div className="grid gap-6 md:grid-cols-2 xl:grid-cols-3">
                <div>
                  <h4 className="text-xs font-semibold uppercase tracking-wide text-slate-500">Profilo & Budget</h4>
                  <div className="mt-2">
                    <DetailList
                      entries={[
                        { label: "Dimensione", value: r.size_class ?? "-" },
                        {
                          label: "Catena",
                          value: typeof r.is_chain === "boolean" ? (r.is_chain ? "Catena" : "Indipendente") : "-",
                        },
                        { label: "Budget", value: r.ad_budget_band ?? "-" },
                        { label: "Fonte budget", value: r.budget_source ?? "-" },
                        { label: "Marketing (metriche)", value: formatPercent(r.marketing_attitude) },
                        { label: "Marketing (LLM)", value: formatPercent(r.facts_marketing_attitude) },
                        { label: "Affinita (LLM)", value: formatPercent(r.facts_umbrella_affinity) },
                      ]}
                    />
                  </div>
                </div>
                <div>
                  <h4 className="text-xs font-semibold uppercase tracking-wide text-slate-500">
                    Digitale & Territorio
                  </h4>
                  <div className="mt-2">
                    <DetailList
                      entries={[
                        { label: "Presenza digitale", value: formatPercent(r.digital_presence) },
                        { label: "Confidenza digitale", value: formatPercent(r.digital_presence_confidence) },
                        { label: "Geo area", value: humanizeLabel(r.geo_distribution_label) },
                        { label: "Fonte geo", value: r.geo_distribution_source ?? "-" },
                        { label: "Densita settore", value: formatPercent(r.sector_density_score) },
                        { label: "N. vicini", value: r.sector_density_neighbors ?? "-" },
                        { label: "Ultimo aggiornamento metriche", value: formatDate(r.metrics_updated_at) },
                      ]}
                    />
                  </div>
                </div>
                <div>
                  <h4 className="text-xs font-semibold uppercase tracking-wide text-slate-500">Fonti & Note</h4>
                  <div className="mt-2 space-y-2">
                    <DetailList
                      entries={[
                        {
                          label: "Website",
                          value: r.website_url ? (
                            <a
                              href={r.website_url}
                              target="_blank"
                              rel="noreferrer"
                              className="text-blue-600 hover:underline"
                            >
                              {r.website_url}
                            </a>
                          ) : (
                            "-"
                          ),
                        },
                        {
                          label: "Social",
                          value:
                            r.social && Object.keys(r.social).length > 0 ? (
                              <ul className="space-y-1">
                                {Object.entries(r.social).map(([network, url]) => (
                                  <li key={network}>
                                    <a
                                      className="text-blue-600 hover:underline"
                                      href={url}
                                      target="_blank"
                                      rel="noreferrer"
                                    >
                                      {network}: {url}
                                    </a>
                                  </li>
                                ))}
                              </ul>
                            ) : (
                              "-"
                            ),
                        },
                        { label: "Confidenza LLM", value: formatPercent(confidence) },
                        { label: "Fonte LLM", value: r.source_provider ?? "-" },
                        { label: "Modello", value: r.source_model ?? "-" },
                        { label: "Aggiornamento LLM", value: formatDate(r.facts_updated_at) },
                      ]}
                    />
                    {r.notes && <p className="text-sm text-slate-700">{r.notes}</p>}
                    {r.provenance && (
                      <pre className="whitespace-pre-wrap rounded bg-white/80 p-3 text-xs text-slate-600 shadow-inner">
                        {JSON.stringify(r.provenance, null, 2)}
                      </pre>
                    )}
                  </div>
                </div>
              </div>
            </div>
          </td>
        </tr>
      )}
    </>
  );
};

export default function App() {
  const [apiBase, setApiBase] = useState<string>(API_DEFAULT);
  const [filters, setFilters] = useState<Filters>({
    city: "",
    category: "",
    geo_label: "",
    size_class: "",
    ad_budget: "",
    is_chain: "any",
    min_affinity: 0,
    min_density: 0,
    min_digital: 0,
    limit: 50,
  });
  const [rows, setRows] = useState<PlaceRow[]>([]);
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [sort, setSort] = useState<SortState | null>(null);
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
    if (filters.geo_label) q.set("geo_label", filters.geo_label);
    if (filters.size_class) q.set("size_class", filters.size_class);
    if (filters.ad_budget) q.set("ad_budget", filters.ad_budget);
    if (filters.is_chain === "yes") q.set("is_chain", "true");
    if (filters.is_chain === "no") q.set("is_chain", "false");
    if (filters.min_affinity > 0) q.set("min_affinity", (filters.min_affinity / 100).toFixed(2));
    if (filters.min_density > 0) q.set("min_density", (filters.min_density / 100).toFixed(2));
    if (filters.min_digital > 0) q.set("min_digital", (filters.min_digital / 100).toFixed(2));
    q.set("limit", String(filters.limit || 50));
    return q.toString();
  }, [filters]);

  const sortedRows = useMemo(() => {
    if (!sort) return rows;
    return [...rows].sort((a, b) => compareByColumn(a, b, sort.column, sort.direction));
  }, [rows, sort]);

  useEffect(() => {
    if (expandedId && !rows.some((row) => row.place_id === expandedId)) {
      setExpandedId(null);
    }
  }, [rows, expandedId]);

  const handleSort = (column: SortColumn) => {
    setSort((prev) => {
      if (prev?.column === column) {
        return { column, direction: prev.direction === "asc" ? "desc" : "asc" };
      }
      return { column, direction: DEFAULT_SORT_DIRECTION[column] };
    });
  };

  const ariaSort = (column: SortColumn): "none" | "ascending" | "descending" => {
    if (sort?.column !== column) return "none";
    return sort.direction === "asc" ? "ascending" : "descending";
  };

  const sortIndicator = (column: SortColumn) => {
    if (sort?.column !== column) return null;
    return (
      <span className="ml-1 text-xs text-slate-500">
        {sort.direction === "asc" ? "▲" : "▼"}
      </span>
    );
  };

  async function fetchHealth() {
    try {
      const r = await fetch(`${apiBase}/health`);
      setHealthy(r.ok);
    } catch {
      setHealthy(false);
    }
  }

  async function fetchCounts() {
    try {
      const r = await fetch(`${apiBase}/counts`);
      if (!r.ok) throw new Error();
      const data = (await r.json()) as CountRow[];
      setCounts(data);
    } catch {
      setCounts(null);
    }
  }

  async function fetchEtlStatus() {
    try {
      const r = await fetch(`${apiBase}/etl/status`);
      if (!r.ok) throw new Error();
      const data = (await r.json()) as ETLStatus;
      setEtl(data);
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

  function resetFilters() {
    setFilters({
      city: "",
      category: "",
      geo_label: "",
      size_class: "",
      ad_budget: "",
      is_chain: "any",
      min_affinity: 0,
      min_density: 0,
      min_digital: 0,
      limit: 50,
    });
    setSort(null);
  }

  useEffect(() => {
    fetchHealth();
    fetchCounts();
    fetchEtlStatus();
  }, [apiBase]);

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
            <h1 className="text-2xl font-bold">CustomerTarget · Metriche Brellò</h1>
            <p className="text-sm text-slate-600">Esplora densità settoriale, affinità al mezzo e presenza digitale arricchite via LLM.</p>
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
              <span className="mb-1.5 md:mb-2">City</span>
              <input
                className="border rounded px-3 py-2.5 focus:outline-none focus:ring-2 focus:ring-blue-200"
                placeholder="Alatri"
                value={filters.city}
                onChange={e => setFilters({ ...filters, city: e.target.value })}
              />
            </label>
            <label className="flex flex-col text-sm">
              <span className="mb-1.5 md:mb-2">Category</span>
              <input
                className="border rounded px-3 py-2.5 focus:outline-none focus:ring-2 focus:ring-blue-200"
                placeholder="restaurant"
                value={filters.category}
                onChange={e => setFilters({ ...filters, category: e.target.value })}
              />
            </label>
            <label className="flex flex-col text-sm">
              <span className="mb-1.5 md:mb-2">Geo label</span>
              <input
                className="border rounded px-3 py-2.5 focus:outline-none focus:ring-2 focus:ring-blue-200"
                placeholder="vicino_brello"
                value={filters.geo_label}
                onChange={e => setFilters({ ...filters, geo_label: e.target.value })}
              />
            </label>
            <label className="flex flex-col text-sm">
              <span className="mb-1.5 md:mb-2">Size class</span>
              <select
                className="border rounded px-3 py-2.5 focus:outline-none focus:ring-2 focus:ring-blue-200"
                value={filters.size_class}
                onChange={e => setFilters({ ...filters, size_class: e.target.value })}
              >
                <option value="">Any</option>
                <option value="micro">Micro</option>
                <option value="piccola">Piccola</option>
                <option value="media">Media</option>
                <option value="grande">Grande</option>
              </select>
            </label>
            <label className="flex flex-col text-sm">
              <span className="mb-1.5 md:mb-2">Budget band</span>
              <select
                className="border rounded px-3 py-2.5 focus:outline-none focus:ring-2 focus:ring-blue-200"
                value={filters.ad_budget}
                onChange={e => setFilters({ ...filters, ad_budget: e.target.value })}
              >
                <option value="">Any</option>
                <option value="basso">Basso</option>
                <option value="medio">Medio</option>
                <option value="alto">Alto</option>
              </select>
            </label>
            <label className="flex flex-col text-sm">
              <span className="mb-1.5 md:mb-2">Catena</span>
              <select
                className="border rounded px-3 py-2.5 focus:outline-none focus:ring-2 focus:ring-blue-200"
                value={filters.is_chain}
                onChange={e => setFilters({ ...filters, is_chain: e.target.value as ChainFilter })}
              >
                <option value="any">Qualsiasi</option>
                <option value="yes">Solo catene</option>
                <option value="no">Solo indipendenti</option>
              </select>
            </label>
            <label className="flex flex-col text-sm">
              <span className="mb-1.5 md:mb-2">Min Affinity (%)</span>
              <input
                type="number"
                min={0}
                max={100}
                step={5}
                className="border rounded px-3 py-2.5 focus:outline-none focus:ring-2 focus:ring-blue-200"
                value={filters.min_affinity}
                onChange={e => setFilters({ ...filters, min_affinity: Number(e.target.value) })}
              />
            </label>
            <label className="flex flex-col text-sm">
              <span className="mb-1.5 md:mb-2">Min Digital (%)</span>
              <input
                type="number"
                min={0}
                max={100}
                step={5}
                className="border rounded px-3 py-2.5 focus:outline-none focus:ring-2 focus:ring-blue-200"
                value={filters.min_digital}
                onChange={e => setFilters({ ...filters, min_digital: Number(e.target.value) })}
              />
            </label>
            <label className="flex flex-col text-sm">
              <span className="mb-1.5 md:mb-2">Min Density (%)</span>
              <input
                type="number"
                min={0}
                max={100}
                step={5}
                className="border rounded px-3 py-2.5 focus:outline-none focus:ring-2 focus:ring-blue-200"
                value={filters.min_density}
                onChange={e => setFilters({ ...filters, min_density: Number(e.target.value) })}
              />
            </label>
            <label className="flex flex-col text-sm">
              <span className="mb-1.5 md:mb-2">Limit</span>
              <input
                type="number"
                min={1}
                max={200}
                className="border rounded px-3 py-2.5 focus:outline-none focus:ring-2 focus:ring-blue-200"
                value={filters.limit}
                onChange={e => setFilters({ ...filters, limit: Number(e.target.value) })}
              />
            </label>
          </div>
          <div className="mt-4 flex flex-wrap items-center gap-2">
            <button
              onClick={search}
              className="ui-btn ui-btn-primary"
              disabled={loading}
            >
              {loading ? "Loading..." : "Search"}
            </button>
            <button
              onClick={resetFilters}
              className="ui-btn ui-btn-ghost"
              disabled={loading}
            >
              Reset
            </button>
          </div>
        </section>

        <section className="mb-6 md:mb-8">
          <div className="flex flex-wrap gap-2">
            <button
              onClick={startOverpass}
              className="ui-btn ui-btn-ghost"
              disabled={etl?.overpass?.status === "running"}
              title="Esegue lo scraper Overpass"
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
                  <th
                    scope="col"
                    className="text-left py-3 px-4"
                    aria-sort={ariaSort("name")}
                  >
                    <button
                      type="button"
                      onClick={() => handleSort("name")}
                      className="flex items-center gap-1 hover:text-blue-600 focus:outline-none"
                    >
                      <span>Name</span>
                      {sortIndicator("name")}
                    </button>
                  </th>
                  <th
                    scope="col"
                    className="text-left py-3 px-4"
                    aria-sort={ariaSort("city")}
                  >
                    <button
                      type="button"
                      onClick={() => handleSort("city")}
                      className="flex items-center gap-1 hover:text-blue-600 focus:outline-none"
                    >
                      <span>City</span>
                      {sortIndicator("city")}
                    </button>
                  </th>
                  <th
                    scope="col"
                    className="text-left py-3 px-4"
                    aria-sort={ariaSort("category")}
                  >
                    <button
                      type="button"
                      onClick={() => handleSort("category")}
                      className="flex items-center gap-1 hover:text-blue-600 focus:outline-none"
                    >
                      <span>Category</span>
                      {sortIndicator("category")}
                    </button>
                  </th>
                  <th className="text-left py-3 px-4">
                    <span>Geo</span>
                  </th>
                  <th
                    scope="col"
                    className="text-right py-3 px-4"
                    aria-sort={ariaSort("umbrella_affinity")}
                  >
                    <button
                      type="button"
                      onClick={() => handleSort("umbrella_affinity")}
                      className="flex items-center justify-end gap-1 hover:text-blue-600 focus:outline-none"
                    >
                      <span>Affinità</span>
                      {sortIndicator("umbrella_affinity")}
                    </button>
                  </th>
                  <th
                    scope="col"
                    className="text-right py-3 px-4"
                    aria-sort={ariaSort("digital_presence")}
                  >
                    <button
                      type="button"
                      onClick={() => handleSort("digital_presence")}
                      className="flex items-center justify-end gap-1 hover:text-blue-600 focus:outline-none"
                    >
                      <span>Digitale</span>
                      {sortIndicator("digital_presence")}
                    </button>
                  </th>
                  <th
                    scope="col"
                    className="text-right py-3 px-4"
                    aria-sort={ariaSort("sector_density_score")}
                  >
                    <button
                      type="button"
                      onClick={() => handleSort("sector_density_score")}
                      className="flex items-center justify-end gap-1 hover:text-blue-600 focus:outline-none"
                    >
                      <span>Densità</span>
                      {sortIndicator("sector_density_score")}
                    </button>
                  </th>
                </tr>
              </thead>
              <tbody>
                {loading ? (
                  <tr><td colSpan={7} className="py-12 px-4 text-center text-slate-500">Loading...</td></tr>
                ) : sortedRows.length === 0 ? (
                  <tr><td colSpan={7} className="py-12 px-4 text-center text-slate-500">No results yet. Adjust filters and press Search.</td></tr>
                ) : (
                  sortedRows.map((r) => (
                    <Row
                      key={r.place_id}
                      r={r}
                      expanded={expandedId === r.place_id}
                      onToggle={() =>
                        setExpandedId((prev) => (prev === r.place_id ? null : r.place_id))
                      }
                    />
                  ))
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
