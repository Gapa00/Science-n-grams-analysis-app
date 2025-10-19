import React, { useEffect, useMemo, useState } from "react";
import { Link, useParams } from "react-router-dom";
import api, { 
  BurstBackendPagination, 
  BurstBackendRow, 
  getBurstLeaderboard,
  getBurstPoints,
  BurstPoint
} from "../api/client";
import NgramFilterPanel, { FilterState } from "../components/NgramFilterPanel";
import FrequencyChart from "../components/FrequencyChart";
import BurstContributionChart from "../components/BurstContributionChart";
import BurstinessLeaderboard, { BurstLeaderboardItem } from "../components/BurstinessLeaderboard";

const DEFAULT_PAGE_SIZE = 50;

type TabType = 'leaderboard';

const BurstPage: React.FC = () => {
  const { method } = useParams<{ method: "macd" | "kleinberg" }>();
  const currentMethod: "macd" | "kleinberg" = method === "macd" ? "macd" : "kleinberg";

  const [activeTab, setActiveTab] = useState<TabType>('leaderboard');

  const [filters, setFilters] = useState<FilterState>({
    domainId: null,
    fieldId: null,
    subfieldId: null,
    nWords: null,
    ngram: null,
    domainName: null,
    fieldName: null,
    subfieldName: null,
  });

  const [start, setStart] = useState<string | null>(null);
  const [end, setEnd] = useState<string | null>(null);

  const [sortBy, setSortBy] = useState<"score" | "ngram" | "normalized_score">("normalized_score");
  const [sortOrder, setSortOrder] = useState<"asc" | "desc">("desc");
  const [page, setPage] = useState<number>(1);
  const [pageSize] = useState<number>(DEFAULT_PAGE_SIZE);

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [data, setData] = useState<BurstLeaderboardItem[]>([]);
  const [pagination, setPagination] = useState<BurstBackendPagination | null>(null);

  const [selectedRow, setSelectedRow] = useState<BurstLeaderboardItem | null>(null);
  
  // Frequency data (original chart)
  const [series, setSeries] = useState<{ date: string; count: number }[]>([]);
  const [seriesError, setSeriesError] = useState<string | null>(null);
  
  // ✅ NEW: Burst contribution data
  const [burstPoints, setBurstPoints] = useState<BurstPoint[]>([]);
  const [burstPointsError, setBurstPointsError] = useState<string | null>(null);
  const [burstPointsLoading, setBurstPointsLoading] = useState(false);

  const queryParams = useMemo(() => {
    const params: Record<string, any> = {
      method: currentMethod,
      page,
      page_size: DEFAULT_PAGE_SIZE,
      sort_order: sortOrder,
    };
    if (filters.subfieldId) params.subfield_id = filters.subfieldId;
    else if (filters.fieldId) params.field_id = filters.fieldId;
    else if (filters.domainId) params.domain_id = filters.domainId;
    if (filters.nWords) params.n_words = filters.nWords;
    if (filters.ngram?.text) params.ngram_text = filters.ngram.text;
    if (start) params.start = start;
    if (end) params.end = end;
    return params;
  }, [currentMethod, filters, page, sortOrder, start, end]);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);

    getBurstLeaderboard(queryParams)
      .then((res) => {
        if (cancelled) return;
        const rows = (res.data || []) as BurstBackendRow[];

        const mapped: BurstLeaderboardItem[] = rows.map((r, idx) => ({
          ngram_id: r.ngram_id,
          ngram: r.text,
          domain: r.domain ?? undefined,
          field: r.field ?? undefined,
          subfield: r.subfield ?? undefined,
          n_words: r.n_words,
          score: r.score,
          normalized_score: r.normalized_score ?? 0,
          rank: (res.pagination.page - 1) * res.pagination.page_size + (idx + 1),
          num_bursts: r.num_bursts ?? undefined,
        }));

        setData(mapped);
        setPagination(res.pagination);
      })
      .catch((err) => {
        if (cancelled) return;
        console.error(err);
        setError("Failed to load leaderboard.");
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [queryParams]);

  const handleSelectRow = (row: BurstLeaderboardItem) => {
    setSelectedRow(row);
    
    // Reset both charts
    setSeries([]);
    setSeriesError(null);
    setBurstPoints([]);
    setBurstPointsError(null);
    setBurstPointsLoading(true);

    // Fetch frequency data (original behavior)
    api
      .get<{ frequency_data: { date: string; count: number }[] }>(`/v1/ngram/${row.ngram_id}/frequency`)
      .then((r) => setSeries(r.data.frequency_data || []))
      .catch((err) => {
        console.error(err);
        setSeriesError("Could not load frequency data for this n-gram.");
      });

    // ✅ NEW: Fetch burst contribution data
    getBurstPoints({
      ngram_id: row.ngram_id,
      method: currentMethod,
      start: start || undefined,
      end: end || undefined,
    })
      .then((res) => {
        setBurstPoints(res.points || []);
      })
      .catch((err) => {
        console.error(err);
        setBurstPointsError("Could not load burst contribution data.");
      })
      .finally(() => {
        setBurstPointsLoading(false);
      });
  };

  const handleSortChange = (field: "score" | "ngram" | "normalized_score") => {
    if (field === sortBy) {
      setSortOrder((o) => (o === "asc" ? "desc" : "asc"));
    } else {
      setSortBy(field);
      setSortOrder(field === "ngram" ? "asc" : "desc");
    }
    setPage(1);
  };

  const handleFilterChange = (next: FilterState) => {
    setFilters(next);
    setPage(1);
  };

  const handleTimeChange = (s: string | null, e: string | null) => {
    setStart(s);
    setEnd(e);
    setPage(1);
  };

  const getStatusText = () => {
    const parts = [];
    parts.push(`Method: **${currentMethod.toUpperCase()}**`);
    const sortLabel = sortBy === "normalized_score" ? "Normalized Score" : 
                     sortBy === "score" ? "Raw Score" : "N-gram";
    parts.push(`Sort: **${sortLabel} ${sortOrder.toUpperCase()}**`);
    if (start || end) {
      parts.push(`Window: **${start ?? "start"} → ${end ?? "end"}**`);
    } else {
      parts.push("**All-time data**");
    }
    if (filters.nWords) {
      parts.push(`Length: **${filters.nWords}-grams**`);
    }
    return parts.join(" • ");
  };

  return (
    <div className="max-w-7xl mx-auto p-4 space-y-6">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h1 className="text-2xl font-semibold">Burst Detection</h1>
        <div className="inline-flex rounded-lg overflow-hidden border">
          <Link
            className={`px-3 py-1 text-sm ${
              currentMethod === "kleinberg" ? "bg-gray-900 text-white" : "bg-white text-gray-700"
            }`}
            to={`/bursts/kleinberg`}
          >
            Kleinberg
          </Link>
          <Link
            className={`px-3 py-1 text-sm ${
              currentMethod === "macd" ? "bg-gray-900 text-white" : "bg-white text-gray-700"
            }`}
            to={`/bursts/macd`}
          >
            MACD
          </Link>
        </div>
      </div>

      <NgramFilterPanel onFilterChange={handleFilterChange} showTimePicker onTimeChange={handleTimeChange} />

      <div className="bg-white rounded-xl shadow">
        <div className="border-b">
          <nav className="flex">
            <button
              onClick={() => setActiveTab('leaderboard')}
              className={`px-6 py-3 text-sm font-medium border-b-2 transition-colors ${
                activeTab === 'leaderboard'
                  ? 'border-blue-500 text-blue-600 bg-blue-50'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
            >
              📊 Leaderboard
            </button>
          </nav>
        </div>

        <div className="p-4">
          {activeTab === 'leaderboard' && (
            <>
              <div className="flex items-center justify-between mb-3">
                <div className="text-sm text-gray-600">
                  <span dangerouslySetInnerHTML={{ __html: getStatusText().replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>') }} />
                </div>
                <div className="flex items-center gap-2">
                  <button
                    onClick={() => setPage((p) => Math.max(1, p - 1))}
                    disabled={!pagination || page <= 1}
                    className="px-3 py-1 rounded bg-gray-100 hover:bg-gray-200 disabled:opacity-50"
                  >
                    Prev
                  </button>
                  <div className="text-sm">
                    Page {pagination?.page ?? 1} / {pagination?.total_pages ?? 1}
                  </div>
                  <button
                    onClick={() => setPage((p) => (pagination ? Math.min(pagination.total_pages, p + 1) : p + 1))}
                    disabled={!pagination || (pagination && page >= pagination.total_pages)}
                    className="px-3 py-1 rounded bg-gray-100 hover:bg-gray-200 disabled:opacity-50"
                  >
                    Next
                  </button>
                </div>
              </div>

              {(start || end || filters.nWords || filters.domainId || filters.fieldId || filters.subfieldId) && (
                <div className="mb-3 p-3 bg-blue-50 border border-blue-200 rounded-lg">
                  <div className="text-sm text-blue-800">
                    <strong>📊 Normalization Scope:</strong> Scores normalized (0-100) within{" "}
                    <strong>{currentMethod.toUpperCase()}</strong>
                    {filters.domainId && <span> • <strong>{filters.domainName || "selected domain"}</strong></span>}
                    {filters.fieldId && !filters.domainId && <span> • <strong>{filters.fieldName || "selected field"}</strong></span>}
                    {filters.subfieldId && !filters.fieldId && <span> • <strong>{filters.subfieldName || "selected subfield"}</strong></span>}
                    {(start || end) && <span> • <strong>{start ?? "start"} to {end ?? "end"}</strong></span>}
                    {filters.nWords && <span> • <strong>{filters.nWords}-grams only</strong></span>}
                  </div>
                </div>
              )}

              {error && <div className="text-red-600 text-sm mb-3">{error}</div>}
              {loading ? (
                <div className="text-sm text-gray-600">Loading…</div>
              ) : (
                <BurstinessLeaderboard
                  data={data}
                  sortBy={sortBy}
                  sortOrder={sortOrder}
                  onSortChange={handleSortChange}
                  onSelectRow={handleSelectRow}
                />
              )}
            </>
          )}
        </div>
      </div>

      {/* ✅ UPDATED: Show both charts when row is selected */}
      {activeTab === 'leaderboard' && selectedRow && (
        <div className="space-y-4">
          {/* Original Frequency Chart */}
          <FrequencyChart
            data={series}
            ngramText={selectedRow.ngram}
            showTooltip
            showAxes
            xLabel="Year"
            yLabel="Document Frequency"
            subtitle={
              seriesError ? (
                <span className="text-red-600">{seriesError}</span>
              ) : start || end ? (
                <>Raw document counts • Shaded: {start ?? "start"} → {end ?? "end"}</>
              ) : (
                <>Raw document counts across all time</>
              )
            }
            height={300}
            intervalStart={start ?? null}
            intervalEnd={end ?? null}
          />

          {/* ✅ NEW: Burst Contribution Chart */}
          {burstPointsLoading ? (
            <div className="bg-white rounded-xl shadow p-4">
              <div className="text-sm text-gray-600">Loading burst contribution data…</div>
            </div>
          ) : burstPointsError ? (
            <div className="bg-white rounded-xl shadow p-4">
              <div className="text-sm text-red-600">{burstPointsError}</div>
            </div>
          ) : burstPoints.length > 0 ? (
            <BurstContributionChart
              data={burstPoints}
              method={currentMethod}
              ngramText={selectedRow.ngram}
              showTooltip
              showAxes
              xLabel="Year"
              yLabel="Burst Contribution"
              subtitle={
                start || end ? (
                  <>
                    Burst detection contributions ({currentMethod.toUpperCase()}) • Shaded: {start ?? "start"} → {end ?? "end"}
                  </>
                ) : (
                  <>Burst detection contributions ({currentMethod.toUpperCase()}) across all time</>
                )
              }
              height={320}
              intervalStart={start ?? null}
              intervalEnd={end ?? null}
            />
          ) : (
            <div className="bg-white rounded-xl shadow p-4">
              <div className="text-sm text-gray-500">No burst contributions detected for this n-gram.</div>
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default BurstPage;