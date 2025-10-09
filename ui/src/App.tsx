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
  llm_raw_response?: unknown;
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
  google_import: JobState;
  pipeline: JobState;
  auto_refresh?: JobState;
};

const DEFAULT_SORT_DIRECTION: Record<SortColumn, SortState["direction"]> = {
  name: "asc",
  city: "asc",
  category: "asc",
  umbrella_affinity: "desc",
  digital_presence: "desc",
  sector_density_score: "desc",
};

const JOB_KEYS = ["google_import", "pipeline", "auto_refresh"] as const;
const JOB_LABEL: Record<typeof JOB_KEYS[number], string> = {
  google_import: "Google Places",
  pipeline: "Pipeline",
  auto_refresh: "Auto Refresh",
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

function humanizeLabel(label: string | null | undefined): string {
  if (!label) return "-";
  const normalized = label.replace(/_/g, " ").trim();
  if (normalized.toLowerCase() === "vicino brello") return "Vicino Brello";
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

type DetailEntry = { label: string; value: React.ReactNode; isAi?: boolean };

const AiBadge: React.FC<{ size?: "xs" | "sm" }> = ({ size = "sm" }) => {
  const sizing =
    size === "xs"
      ? "px-2 py-0 text-[0.58rem]"
      : "px-2.5 py-0.5 text-[0.65rem]";
  return (
    <span
      className={`inline-flex shrink-0 items-center rounded-full border border-violet-200 bg-violet-50 font-semibold uppercase tracking-[0.18em] text-violet-700 whitespace-nowrap ${sizing}`}
    >
      AI
    </span>
  );
};

const DetailList: React.FC<{ entries: DetailEntry[] }> = ({ entries }) => (
  <dl className="grid grid-cols-1 gap-y-3 text-sm">
    {entries.map((entry, idx) => {
      const rawValue = entry.value ?? "-";
      const isPlainText =
        typeof rawValue === "string" ||
        typeof rawValue === "number" ||
        typeof rawValue === "boolean";
      const labelHasLlm = /llm/i.test(entry.label);
      const valueHasLlm = typeof rawValue === "string" && /llm/i.test(rawValue);
      const showAi = entry.isAi ?? (labelHasLlm || valueHasLlm);
      const displayValue = isPlainText ? (
        <span className="font-semibold text-slate-900 leading-snug">{rawValue}</span>
      ) : (
        rawValue
      );
      return (
        <div
          key={`${entry.label}-${idx}`}
          className="flex flex-col gap-1 sm:flex-row sm:items-start sm:gap-4"
        >
          <dt className="text-left text-[0.7rem] font-semibold uppercase tracking-[0.18em] text-slate-500 sm:w-48 sm:flex-shrink-0">
            {entry.label}
          </dt>
          <dd className="min-w-0 flex-1 break-words text-left text-slate-700 leading-snug">
            <div className={`flex ${isPlainText ? "items-center" : "items-start"} gap-2`}>
              <div className={`${isPlainText ? "" : "min-w-0"} flex-1`}>{displayValue}</div>
              {showAi && <AiBadge size="xs" />}
            </div>
          </dd>
        </div>
      );
    })}
  </dl>
);

const MetricBar: React.FC<{ value?: number | null }> = ({ value }) => {
  const pct = clamp01(value);
  if (pct == null) {
    return <span className="text-slate-400 font-medium">-</span>;
  }
  const percent = Math.round(pct * 100);
  return (
    <div className="flex flex-col gap-1">
      <span className="text-sm font-semibold text-slate-900">{percent}%</span>
      <div className="relative h-1.5 w-full overflow-hidden rounded-full bg-slate-200">
        <span
          className="absolute inset-y-0 left-0 rounded-full bg-gradient-to-r from-sky-400 via-indigo-500 to-indigo-600"
          style={{ width: `${percent}%` }}
        />
      </div>
    </div>
  );
};

type RowProps = {
  r: PlaceRow;
  expanded: boolean;
  onToggle: () => void;
};

const Row: React.FC<RowProps> = ({ r, expanded, onToggle }) => {
  const confidence = r.facts_confidence_override ?? r.facts_confidence ?? r.digital_presence_confidence;
  const detailId = `details-${sanitizeId(r.place_id)}`;

  let provenanceReasoning: string | null = null;
  let provenanceRest: Record<string, unknown> | null = null;
  if (r.provenance && typeof r.provenance === "object" && !Array.isArray(r.provenance)) {
    const rest: Record<string, unknown> = {};
    Object.entries(r.provenance).forEach(([key, value]) => {
      if (key === "reasoning" && typeof value === "string" && value.trim().length > 0) {
        provenanceReasoning = value.trim();
      } else {
        rest[key] = value;
      }
    });
    if (Object.keys(rest).length > 0) {
      provenanceRest = rest;
    }
  }

  const aiHints = new Set<string>();
  if (provenanceRest) {
    Object.entries(provenanceRest).forEach(([key, value]) => {
      if (typeof value === "string" && value.toLowerCase().includes("llm")) {
        aiHints.add(key.toLowerCase());
      }
    });
  }
  if (typeof r.budget_source === "string" && r.budget_source.toLowerCase().includes("llm")) {
    aiHints.add("budget");
  }

  const sizeAi =
    aiHints.has("size_class_source") ||
    aiHints.has("size_class") ||
    aiHints.has("dimensione");
  const chainAi = aiHints.has("is_chain_source") || aiHints.has("is_chain");
  const budgetAi = aiHints.has("budget") || aiHints.has("ad_budget_band");

  const highlightChips = [
    r.size_class && {
      key: "size",
      label: "Dimensione",
      value: r.size_class,
      className: "border-slate-200 bg-slate-100 text-slate-700",
      isAi: sizeAi,
    },
    typeof r.is_chain === "boolean" && {
      key: "chain",
      label: "Formato",
      value: r.is_chain ? "Catena" : "Indipendente",
      className: r.is_chain
        ? "border-indigo-200 bg-indigo-50 text-indigo-700"
        : "border-emerald-200 bg-emerald-50 text-emerald-700",
      isAi: chainAi,
    },
    r.ad_budget_band && {
      key: "budget",
      label: "Budget",
      value: r.ad_budget_band,
      className:
        r.ad_budget_band.toLowerCase() === "alto"
          ? "border-amber-200 bg-amber-50 text-amber-700"
          : r.ad_budget_band.toLowerCase() === "medio"
            ? "border-sky-200 bg-sky-50 text-sky-700"
            : "border-slate-200 bg-slate-50 text-slate-700",
      isAi: budgetAi,
    },
    r.geo_distribution_label && {
      key: "geo",
      label: "Geo area",
      value: humanizeLabel(r.geo_distribution_label),
      className: "border-blue-200 bg-blue-50 text-blue-700",
      isAi: aiHints.has("geo_distribution_source"),
    },
  ].filter(Boolean) as Array<{
    key: string;
    label: string;
    value: React.ReactNode;
    className: string;
    isAi?: boolean;
  }>;

  const prettyRawResponse = useMemo(() => {
    const raw = r.llm_raw_response;
    const stringify = (value: unknown): string | null => {
      try {
        return JSON.stringify(value, null, 2);
      } catch {
        return null;
      }
    };

    const tryParse = (input: string): string | null => {
      try {
        const parsed = JSON.parse(input);
        if (typeof parsed === "string") {
          const nested = tryParse(parsed);
          return nested ?? parsed;
        }
        return stringify(parsed);
      } catch {
        return null;
      }
    };

    const stripFence = (input: string): string => {
      let out = input.trim();
      if (out.startsWith("```")) {
        out = out.replace(/^```(?:json)?/i, "").trim();
        if (out.endsWith("```")) {
          out = out.slice(0, -3).trim();
        }
      }
      return out;
    };

    const decodeEscapes = (input: string): string => {
      try {
        const escaped = input
          .replace(/\\/g, "\\\\")
          .replace(/"/g, '\\"')
          .replace(/\r?\n/g, "\\n");
        return JSON.parse(`"${escaped}"`);
      } catch {
        return input;
      }
    };

    if (raw == null) {
      return null;
    }

    if (typeof raw === "string") {
      const cleaned = stripFence(raw);
      if (!cleaned) {
        return null;
      }

      let pretty = tryParse(cleaned);
      if (pretty) {
        return pretty;
      }

      if (
        (cleaned.startsWith('"') && cleaned.endsWith('"')) ||
        (cleaned.startsWith("'") && cleaned.endsWith("'"))
      ) {
        const unwrapped = cleaned.slice(1, -1);
        pretty = tryParse(unwrapped);
        if (pretty) {
          return pretty;
        }
      }

      const decoded = decodeEscapes(cleaned);
      if (decoded !== cleaned) {
        const nested = tryParse(decoded);
        if (nested) {
          return nested;
        }
        return decoded;
      }

      return decodeEscapes(cleaned);
    }

    const asJson = stringify(raw);
    return asJson ?? String(raw);
  }, [r.llm_raw_response]);
  const hasRawResponse = Boolean(prettyRawResponse && prettyRawResponse.trim().length > 0);
  const shouldRenderLowerGrid = Boolean(r.notes || provenanceReasoning || provenanceRest || hasRawResponse);

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
        <tr className="border-b">
          <td colSpan={7} className="px-4 pb-6 pt-0">
            <div id={detailId} className="pt-4">
              <div className="rounded-2xl border border-slate-200 bg-white shadow-sm">
                <div className="flex flex-col gap-6 p-4 md:p-6">
                  {highlightChips.length > 0 && (
                    <>
                      <div className="flex flex-wrap gap-2 text-xs font-semibold">
                        {highlightChips.map((chip) => (
                          <span
                            key={chip.key}
                            className={`inline-flex max-w-full flex-wrap items-center gap-1 rounded-full border px-3 py-1 ${chip.className}`}
                            title={String(chip.value)}
                          >
                            <span className="text-[0.65rem] uppercase tracking-[0.2em] text-slate-500">
                              {chip.label}
                            </span>
                            <span className="text-sm font-semibold text-slate-900 break-words leading-tight">
                              {chip.value}
                            </span>
                            {chip.isAi && <AiBadge size="xs" />}
                          </span>
                        ))}
                      </div>
                      <div className="h-px w-full bg-slate-100" />
                    </>
                  )}
                  <div className="grid gap-4 md:grid-cols-2">
                    <section className="rounded-2xl border border-slate-100 bg-slate-50/60 p-4 shadow-inner">
                      <h4 className="text-left text-xs font-semibold uppercase tracking-wide text-slate-500">
                        Profilo & Budget
                      </h4>
                      <div className="mt-3">
                        <DetailList
                          entries={[
                            {
                              label: "Fonte budget",
                              value: r.budget_source ?? "-",
                              isAi: /llm/i.test(r.budget_source ?? ""),
                            },
                            { label: "Marketing (metriche)", value: <MetricBar value={r.marketing_attitude} /> },
                            {
                              label: "Marketing (LLM)",
                              value: <MetricBar value={r.facts_marketing_attitude} />,
                              isAi: true,
                            },
                            {
                              label: "Affinita (LLM)",
                              value: <MetricBar value={r.facts_umbrella_affinity} />,
                              isAi: true,
                            },
                          ]}
                        />
                      </div>
                    </section>
                    <section className="rounded-2xl border border-slate-100 bg-slate-50/60 p-4 shadow-inner">
                      <h4 className="text-left text-xs font-semibold uppercase tracking-wide text-slate-500">
                        Digitale & Territorio
                      </h4>
                      <div className="mt-3">
                        <DetailList
                          entries={[
                            { label: "Presenza digitale", value: <MetricBar value={r.digital_presence} /> },
                            { label: "Confidenza digitale", value: <MetricBar value={r.digital_presence_confidence} /> },
                            { label: "Geo area", value: humanizeLabel(r.geo_distribution_label) },
                            { label: "Fonte geo", value: r.geo_distribution_source ?? "-" },
                            { label: "Densita settore", value: <MetricBar value={r.sector_density_score} /> },
                            { label: "N. vicini", value: r.sector_density_neighbors ?? "-" },
                            { label: "Ultimo aggiornamento metriche", value: formatDate(r.metrics_updated_at) },
                          ]}
                        />
                      </div>
                    </section>
                  </div>
                  <section className="rounded-2xl border border-slate-100 bg-slate-50/60 p-4 shadow-inner">
                    <h4 className="text-left text-xs font-semibold uppercase tracking-wide text-slate-500">
                      Fonti & Note
                    </h4>
                    <div className="mt-3">
                      <DetailList
                        entries={[
                          {
                            label: "Website",
                            value: r.website_url ? (
                              <a
                                href={r.website_url}
                                target="_blank"
                                rel="noreferrer"
                                className="break-words text-blue-600 hover:text-blue-700 hover:underline"
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
                                <ul className="space-y-2 text-slate-700">
                                  {Object.entries(r.social).map(([network, url]) => (
                                    <li key={network} className="space-y-1">
                                      <span className="text-[0.65rem] font-semibold uppercase tracking-[0.18em] text-slate-500">
                                        {network}
                                      </span>
                                      <div className="break-words">
                                        <a
                                          className="break-words text-blue-600 hover:text-blue-700 hover:underline"
                                          href={url}
                                          target="_blank"
                                          rel="noreferrer"
                                        >
                                          {url}
                                        </a>
                                      </div>
                                    </li>
                                  ))}
                                </ul>
                              ) : (
                                "-"
                              ),
                            isAi: true,
                          },
                          {
                            label: "Confidenza LLM",
                            value: confidence != null ? <ConfidencePill value={confidence} /> : "-",
                            isAi: true,
                          },
                          { label: "Fonte LLM", value: r.source_provider ?? "-", isAi: true },
                          { label: "Modello", value: r.source_model ?? "-", isAi: true },
                          { label: "Aggiornamento LLM", value: formatDate(r.facts_updated_at), isAi: true },
                        ]}
                      />
                    </div>
                  </section>
                  {shouldRenderLowerGrid && (
                    <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
                      {r.notes && (
                        <div className="rounded-xl bg-slate-50 p-4 text-sm leading-relaxed text-slate-700">
                          <h5 className="text-xs font-semibold uppercase tracking-wide text-slate-500">Note</h5>
                          <p className="mt-2">{r.notes}</p>
                        </div>
                      )}
                      {(provenanceReasoning || provenanceRest) && (
                        <div className="rounded-xl border border-slate-100 bg-slate-900/5 p-4 text-xs text-slate-600">
                          <h5 className="text-xs font-semibold uppercase tracking-wide text-slate-500">
                            Provenienza LLM
                          </h5>
                          {provenanceReasoning && (
                            <blockquote className="mt-2 rounded-lg border border-slate-200 bg-white p-3 text-sm leading-relaxed text-slate-700 shadow-sm">
                              <span className="block text-[0.65rem] font-semibold uppercase tracking-[0.18em] text-slate-400">
                                Reasoning
                              </span>
                              <span className="mt-1 block leading-relaxed">{provenanceReasoning}</span>
                            </blockquote>
                          )}
                          {provenanceRest && (
                            <pre className="mt-3 whitespace-pre-wrap break-words rounded-lg border border-slate-200 bg-white p-3 leading-relaxed text-slate-600">
                              {JSON.stringify(provenanceRest, null, 2)}
                            </pre>
                          )}
                        </div>
                      )}
                      {hasRawResponse && prettyRawResponse && (
                        <div className="rounded-xl border border-slate-100 bg-white p-4 text-xs text-slate-700 shadow-inner md:col-span-2 lg:col-span-1">
                          <h5 className="text-xs font-semibold uppercase tracking-wide text-slate-500">
                            Response LLM (raw)
                          </h5>
                          <pre className="mt-2 max-h-96 min-h-[200px] overflow-auto whitespace-pre-wrap break-words rounded-lg border border-slate-200 bg-slate-900/5 p-3 font-mono text-[0.7rem] leading-relaxed text-slate-700">
                            {prettyRawResponse}
                          </pre>
                        </div>
                      )}
                    </div>
                  )}
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
  const [googleLocation, setGoogleLocation] = useState<string>("");
  const [googleLat, setGoogleLat] = useState<string>("");
  const [googleLng, setGoogleLng] = useState<string>("");
  const [googleRadius, setGoogleRadius] = useState<string>("");
  const [googleLimit, setGoogleLimit] = useState<string>("");
  const [googleSleepSeconds, setGoogleSleepSeconds] = useState<string>("2");
  const [googleQueries, setGoogleQueries] = useState<string>("");
  const [googleLoading, setGoogleLoading] = useState<boolean>(false);
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
        data.google_import?.status === "ok" ||
        data.auto_refresh?.status === "ok"
      ) {
        fetchCounts();
      }
    } catch {
      setEtl(null);
    }
  }

  async function startGoogleImport() {
    const queries = googleQueries
      .split(/[\r\n,]/)
      .map((q) => q.trim())
      .filter((q) => q.length > 0);
    const payload: {
      location?: string;
      lat?: number;
      lng?: number;
      radius?: number;
      limit?: number;
      sleep_seconds?: number;
      queries?: string[];
    } = {};

    if (queries.length > 0) {
      payload.queries = queries;
    }

    const locationValue = googleLocation.trim();
    if (locationValue) {
      payload.location = locationValue;
    }

    const latValue = googleLat.trim();
    const lngValue = googleLng.trim();
    if (latValue || lngValue) {
      if (!latValue || !lngValue) {
        alert("Specificare sia lat che lng oppure nessuno dei due.");
        return;
      }
      const latNum = Number(latValue);
      const lngNum = Number(lngValue);
      if (!Number.isFinite(latNum) || !Number.isFinite(lngNum)) {
        alert("Latitudine o longitudine non valide.");
        return;
      }
      payload.lat = latNum;
      payload.lng = lngNum;
    }

    if (!payload.location && payload.lat === undefined) {
      alert("Fornisci una location oppure la coppia lat/lng.");
      return;
    }

    const radiusValue = googleRadius.trim();
    if (radiusValue) {
      const radiusNum = Number(radiusValue);
      if (!Number.isFinite(radiusNum) || radiusNum <= 0) {
        alert("Il raggio deve essere un numero intero positivo.");
        return;
      }
      payload.radius = Math.round(radiusNum);
    }

    const limitValue = googleLimit.trim();
    if (limitValue) {
      const limitNum = Number(limitValue);
      if (!Number.isFinite(limitNum) || limitNum <= 0) {
        alert("Il limite deve essere un numero intero positivo.");
        return;
      }
      payload.limit = Math.round(limitNum);
    }

    const sleepValue = googleSleepSeconds.trim();
    if (sleepValue) {
      const sleepNum = Number(sleepValue);
      if (!Number.isFinite(sleepNum) || sleepNum < 0) {
        alert("Sleep seconds deve essere un numero maggiore o uguale a zero.");
        return;
      }
      payload.sleep_seconds = sleepNum;
    }

    setGoogleLoading(true);
    try {
      const r = await fetch(`${apiBase}/etl/google_places/start`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!r.ok) throw new Error(await r.text());
      await fetchEtlStatus();
      startPolling();
    } catch (e) {
      alert(`Google Places: ${String(e)}`);
    } finally {
      setGoogleLoading(false);
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

  async function startAutoRefresh() {
    try {
      const r = await fetch(`${apiBase}/automation/auto_refresh/start`, { method: "POST" });
      if (!r.ok) throw new Error(await r.text());
      await fetchEtlStatus();
      startPolling();
    } catch (e) {
      alert(`Auto Refresh: ${String(e)}`);
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
    const anyRunning = JOB_KEYS.some((k) => etl?.[k]?.status === "running");
    if (anyRunning) startPolling(); else stopPolling();
    return () => stopPolling();
  }, [etl]);

  return (
    <div className="min-h-screen bg-white text-slate-900">
      <div className="max-w-6xl mx-auto p-4 md:p-6 lg:p-8">
        <header className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3 mb-6 md:mb-8">
          <div>
            <h1 className="text-2xl font-bold">CustomerTarget - Metriche Brello</h1>
            <p className="text-sm text-slate-600">Esplora densita settoriale, affinita al mezzo e presenza digitale arricchite via LLM.</p>
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
          <div className="grid gap-4 lg:grid-cols-2">
            <div className="border rounded-xl p-4 space-y-3 bg-slate-50">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <h2 className="text-sm font-semibold text-slate-800">Google Places Import</h2>
                  <p className="text-xs text-slate-600">
                    Avvia <code className="text-xs">etl/google_places.py</code> per popolare <code className="text-xs">places_raw</code>.
                  </p>
                </div>
                <span className={`text-xs px-2 py-1 rounded-full ${
                  etl?.google_import?.status === "running" ? "bg-amber-100 text-amber-700" :
                  etl?.google_import?.status === "ok" ? "bg-emerald-100 text-emerald-700" :
                  etl?.google_import?.status === "error" ? "bg-rose-100 text-rose-700" :
                  "bg-slate-100 text-slate-600"
                }`}>
                  {etl?.google_import?.status ?? "idle"}
                </span>
              </div>
              <div className="grid gap-3 sm:grid-cols-2">
                <label className="flex flex-col text-xs font-medium text-slate-600">
                  <span className="mb-1">Location</span>
                  <input
                    className="ui-input"
                    placeholder="Es. Alatri, Italia"
                    value={googleLocation}
                    onChange={(e) => setGoogleLocation(e.target.value)}
                  />
                </label>
                <label className="flex flex-col text-xs font-medium text-slate-600">
                  <span className="mb-1">Radius (m)</span>
                  <input
                    className="ui-input"
                    type="number"
                    min={1}
                    placeholder="Auto"
                    value={googleRadius}
                    onChange={(e) => setGoogleRadius(e.target.value)}
                  />
                </label>
              </div>
              <div className="grid gap-3 sm:grid-cols-2">
                <label className="flex flex-col text-xs font-medium text-slate-600">
                  <span className="mb-1">Lat</span>
                  <input
                    className="ui-input"
                    type="number"
                    step="any"
                    placeholder="Facoltativo"
                    value={googleLat}
                    onChange={(e) => setGoogleLat(e.target.value)}
                  />
                </label>
                <label className="flex flex-col text-xs font-medium text-slate-600">
                  <span className="mb-1">Lng</span>
                  <input
                    className="ui-input"
                    type="number"
                    step="any"
                    placeholder="Facoltativo"
                    value={googleLng}
                    onChange={(e) => setGoogleLng(e.target.value)}
                  />
                </label>
              </div>
              <div className="grid gap-3 sm:grid-cols-2">
                <label className="flex flex-col text-xs font-medium text-slate-600">
                  <span className="mb-1">Limit</span>
                  <input
                    className="ui-input"
                    type="number"
                    min={1}
                    placeholder="Es. 200"
                    value={googleLimit}
                    onChange={(e) => setGoogleLimit(e.target.value)}
                  />
                </label>
                <label className="flex flex-col text-xs font-medium text-slate-600">
                  <span className="mb-1">Sleep seconds</span>
                  <input
                    className="ui-input"
                    type="number"
                    min={0}
                    step="0.1"
                    placeholder="Default 2"
                    value={googleSleepSeconds}
                    onChange={(e) => setGoogleSleepSeconds(e.target.value)}
                  />
                </label>
              </div>
              <label className="flex flex-col text-xs font-medium text-slate-600">
                <span className="mb-1">Queries Google Places</span>
                <textarea
                  className="ui-input h-24 resize-y"
                  placeholder="Una query per riga (es. ristorante)"
                  value={googleQueries}
                  onChange={(e) => setGoogleQueries(e.target.value)}
                />
                <p className="mt-1 text-[11px] text-slate-500">
                  Lascia vuoto per usare il file <code>etl/queries/google_places_queries.txt</code>.
                </p>
              </label>
              <div className="flex flex-wrap items-center gap-2">
                <button
                  onClick={startGoogleImport}
                  className="ui-btn ui-btn-primary"
                  disabled={googleLoading || etl?.google_import?.status === "running"}
                  title="Esegue l'import Google Places"
                >
                  {googleLoading ? "Running..." : "Run Google Import"}
                </button>
                <button
                  onClick={() => fetchEtlStatus()}
                  className="ui-btn ui-btn-ghost"
                >
                  Refresh Status
                </button>
              </div>
              <p className="text-xs text-slate-500">
                Compila una location oppure la coppia lat/lng. Le query sono facoltative: senza input useremo <code>etl/queries/google_places_queries.txt</code>.
              </p>
            </div>
            <div className="border rounded-xl p-4 space-y-3">
              <h2 className="text-sm font-semibold text-slate-800">Pipeline e automazioni</h2>
              <p className="text-xs text-slate-600">
                Lancia gli step SQL e il ciclo enrichment/metriche.
              </p>
              <div className="flex flex-wrap gap-2">
                <button
                  onClick={startPipeline}
                  className="ui-btn ui-btn-ghost"
                  disabled={etl?.pipeline?.status === "running"}
                  title="Esegue gli step SQL della pipeline"
                >
                  Run Pipeline
                </button>
                <button
                  onClick={startAutoRefresh}
                  className="ui-btn ui-btn-ghost"
                  disabled={etl?.auto_refresh?.status === "running"}
                  title="Esegue arricchimento LLM e ricalcolo metriche"
                >
                  Run Auto Refresh
                </button>
              </div>
            </div>
          </div>
        </section>

        {etl && (
          <section className="mb-6">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {JOB_KEYS.map((k) => {
                const job = etl[k];
                if (!job) return null;
                return (
                  <div key={k} className="border rounded-xl p-3">
                    <div className="flex items-center justify-between mb-2">
                      <h3 className="font-semibold">{JOB_LABEL[k]}</h3>
                      <span className={`text-xs px-2 py-1 rounded-full ${
                        job.status === "running" ? "bg-amber-100 text-amber-700" :
                        job.status === "ok" ? "bg-emerald-100 text-emerald-700" :
                        job.status === "error" ? "bg-rose-100 text-rose-700" :
                        "bg-slate-100 text-slate-700"
                      }`}>
                        {job.status}
                      </span>
                    </div>
                    <div className="text-xs text-slate-600 mb-2">
                      <div>started: {job.started_at ?? "-"}</div>
                      <div>ended: {job.ended_at ?? "-"}</div>
                      {typeof job.last_rc === "number" && <div>rc: {job.last_rc}</div>}
                    </div>
                    {job.last_lines && job.last_lines.length > 0 && (
                      <pre className="text-xs bg-slate-50 border rounded p-2 max-h-48 overflow-auto">
                        {job.last_lines.slice(-20).join("\n")}
                      </pre>
                    )}
                  </div>
                );
              })}
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
                      <span>Affinita</span>
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
                      <span>Densita</span>
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
