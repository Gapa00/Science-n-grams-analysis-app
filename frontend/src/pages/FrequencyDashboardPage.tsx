// File: src/pages/FrequencyDashboardPage.tsx
import { useEffect, useState, useCallback, useRef } from "react";
import api from "../api/client";
import NgramFilterPanel from "../components/NgramFilterPanel";
import LeaderboardTable, { NgramRow } from "../components/LeaderboardTable";
import FrequencyChart from "../components/FrequencyChart";

function FrequencyDashboardPage() {
  // Filters default so we fetch on first load
  const [filters, setFilters] = useState<any>({
    domainId: null,
    fieldId: null,
    subfieldId: null,
    ngram: null,
    nWords: null,
  });

  const [ngrams, setNgrams] = useState<NgramRow[]>([]);
  const [selectedNgram, setSelectedNgram] = useState<NgramRow | null>(null);
  const [frequencyData, setFrequencyData] = useState<any[]>([]);

  // Pagination (committed page vs pending while loading)
  const [page, setPage] = useState<number>(1);
  const [pendingPage, setPendingPage] = useState<number>(1);
  const [totalPages, setTotalPages] = useState<number>(1);

  // Sorting: dynamic default based on whether we're searching
  const [sortBy, setSortBy] = useState<string>("text");
  const [sortOrder, setSortOrder] = useState<"asc" | "desc">("asc");

  const [isLoading, setIsLoading] = useState<boolean>(false);

  // Guards to avoid stale responses overwriting fresh ones
  const lastReqId = useRef(0);

  // Check if we're currently searching
  const isSearching = Boolean(filters.ngram?.text);

  const buildParams = (pageArg: number) => {
    const params: any = {
      page: pageArg,
      page_size: 100,
      sort_by: sortBy,
      sort_order: sortOrder,
    };
    if (filters.subfieldId) params.subfield_id = filters.subfieldId;
    else if (filters.fieldId) params.field_id = filters.fieldId;
    else if (filters.domainId) params.domain_id = filters.domainId;
    if (filters.nWords) params.n_words = filters.nWords;
    if (filters.ngram?.text) params.ngram_text = filters.ngram.text; // text match
    return params;
  };

  const fetchLeaderboard = useCallback(
    async (pageArg: number) => {
      const reqId = ++lastReqId.current;
      try {
        setIsLoading(true);
        setPendingPage(pageArg); // reflect intent immediately
        const res = await api.get("/v1/leaderboard", {
          params: buildParams(pageArg),
        });
        if (reqId !== lastReqId.current) return; // ignore stale response
        setNgrams(res.data.data);
        setTotalPages(res.data.pagination.total_pages);
        setPage(pageArg); // commit page once data arrives
      } finally {
        if (reqId === lastReqId.current) setIsLoading(false);
      }
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [sortBy, sortOrder, filters]
  );

  // Initial load + whenever filters/sort change, go to page 1
  useEffect(() => {
    fetchLeaderboard(1);
  }, [fetchLeaderboard]);

  // Frequency series for selected ngram
  useEffect(() => {
    if (!selectedNgram) return;
    api.get(`/v1/ngram/${selectedNgram.id}/frequency`).then((res) => {
      setFrequencyData(res.data.frequency_data);
    });
  }, [selectedNgram]);

  const handlePrevPage = () => {
    if (page <= 1 || isLoading) return;
    fetchLeaderboard(page - 1);
  };

  const handleNextPage = () => {
    if (page >= totalPages || isLoading) return;
    fetchLeaderboard(page + 1);
  };

  const handleSortChange = (field: string) => {
    if (field === sortBy) {
      setSortOrder((prev) => (prev === "asc" ? "desc" : "asc"));
    } else {
      setSortBy(field);
      setSortOrder("desc");
    }
    // Effect will trigger fetchLeaderboard(1)
  };

  const handleFilterChange = useCallback((newFilters: any) => {
    setFilters((prev: any) => {
      const changed = JSON.stringify(prev) !== JSON.stringify(newFilters);
      if (changed) {
        setSelectedNgram(null);
        setFrequencyData([]);
      }
      return newFilters;
    });
  }, []);

  return (
    <div className="p-6">
      {/* Page Header */}
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-gray-900 mb-2">Frequency Dashboard</h1>
        <p className="text-gray-600">
          Explore scientific n-grams and their frequency patterns over time. Filter by domain, field, or subfield to find specific terms.
        </p>
      </div>

      <NgramFilterPanel onFilterChange={handleFilterChange} />

      {/* Search feedback */}
      {isSearching && (
        <div className="mb-4 p-3 bg-blue-50 border border-blue-200 rounded-lg">
          <div className="flex items-center gap-2">
            <svg className="h-5 w-5 text-blue-600" fill="currentColor" viewBox="0 0 20 20">
              <path fillRule="evenodd" d="M8 4a4 4 0 100 8 4 4 0 000-8zM2 8a6 6 0 1110.89 3.476l4.817 4.817a1 1 0 01-1.414 1.414l-4.816-4.816A6 6 0 012 8z" clipRule="evenodd" />
            </svg>
            <span className="text-blue-800 font-medium">
              Searching for: "{filters.ngram.text}"
            </span>
            <span className="text-blue-600 text-sm">
              (Results ordered by relevance - exact matches first)
            </span>
          </div>
        </div>
      )}

      {/* Loading indicator */}
      {isLoading && (
        <div className="mb-2 flex items-center gap-2 text-sm text-gray-600">
          <svg className="animate-spin h-4 w-4" viewBox="0 0 24 24">
            <circle
              className="opacity-25"
              cx="12"
              cy="12"
              r="10"
              stroke="currentColor"
              strokeWidth="4"
              fill="none"
            />
            <path
              className="opacity-75"
              fill="currentColor"
              d="M4 12a8 8 0 018-8v4A4 4 0 008 12H4z"
            />
          </svg>
          <span>Loading…</span>
        </div>
      )}

      <div className={isLoading ? "opacity-60 pointer-events-none" : ""}>
        <LeaderboardTable
          data={ngrams}
          onSelectNgram={setSelectedNgram}
          sortBy={sortBy}
          sortOrder={sortOrder}
          onSortChange={handleSortChange}
        />
      </div>

      <div className="mt-4 flex justify-between items-center text-sm">
        <button
          onClick={handlePrevPage}
          disabled={page === 1 || isLoading}
          className="px-3 py-1 bg-gray-200 rounded disabled:opacity-50"
        >
          Previous
        </button>
        <span>
          Page {isLoading ? pendingPage : page} of {totalPages}
        </span>
        <button
          onClick={handleNextPage}
          disabled={page === totalPages || isLoading}
          className="px-3 py-1 bg-gray-200 rounded disabled:opacity-50"
        >
          Next
        </button>
      </div>

      {selectedNgram && frequencyData && frequencyData.length > 0 && (
        <FrequencyChart
          data={frequencyData}
          ngramText={selectedNgram.text}
          showTooltip
          showAxes
          subtitle={[
            selectedNgram.domain,
            selectedNgram.field,
            selectedNgram.subfield
          ].filter(Boolean).join(" | ")}
        />
      )}
    </div>
  );
}

export default FrequencyDashboardPage;