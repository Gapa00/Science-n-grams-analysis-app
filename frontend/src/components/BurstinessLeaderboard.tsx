import React from "react";

export type BurstLeaderboardItem = {
  ngram_id: number;
  ngram: string;
  domain?: string;
  field?: string;
  subfield?: string;
  n_words: number;
  score: number;
  normalized_score: number;  // ✅ NEW: 0-100 normalized score
  rank?: number;          // computed client-side for display
  num_bursts?: number;    // only present for global leaderboard
};

type Props = {
  data: BurstLeaderboardItem[];
  sortBy: "score" | "ngram" | "normalized_score";  // ✅ NEW: Add normalized_score as sort option
  sortOrder: "asc" | "desc";
  onSortChange: (field: "score" | "ngram" | "normalized_score") => void;
  onSelectRow?: (row: BurstLeaderboardItem) => void;
};

const fmtNum = (n: number | null | undefined) =>
  typeof n === "number" ? new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 }).format(n) : "—";

const fmtScore = (n: number | null | undefined) =>
  typeof n === "number" ? new Intl.NumberFormat(undefined, { maximumFractionDigits: 3 }).format(n) : "—";

const BurstinessLeaderboard: React.FC<Props> = ({ data, sortBy, sortOrder, onSortChange, onSelectRow }) => {
  const renderSortIcon = (field: "score" | "ngram" | "normalized_score") => (
    sortBy === field ? (sortOrder === "asc" ? "▲" : "▼") : null
  );

  return (
    <div className="overflow-x-auto">
      <table className="min-w-full table-auto text-sm">
        <thead className="bg-gray-100 text-left">
          <tr>
            <th className="p-2">#</th>
            <th className="p-2 cursor-pointer" onClick={() => onSortChange("ngram")}>
              N-gram {renderSortIcon("ngram")}
            </th>
            {/* ✅ NEW: Normalized Score Column (priority position) */}
            <th className="p-2 cursor-pointer bg-blue-50" onClick={() => onSortChange("normalized_score")}>
              <div className="flex items-center gap-1">
                <span className="font-semibold text-blue-700">Norm Score</span>
                <span className="text-xs text-blue-600">(0-100)</span>
                {renderSortIcon("normalized_score")}
              </div>
            </th>
            <th className="p-2 cursor-pointer" onClick={() => onSortChange("score")}>
              Raw Score {renderSortIcon("score")}
            </th>
            <th className="p-2">Subfield</th>
            <th className="p-2">Field</th>
            <th className="p-2">Domain</th>
            <th className="p-2">Global Bursts</th>
          </tr>
        </thead>
        <tbody>
          {data.map((row, idx) => (
            <tr
              key={row.ngram_id}
              className="hover:bg-gray-50 cursor-pointer"
              onClick={() => onSelectRow?.(row)}
            >
              <td className="p-2 text-gray-500">{idx + 1}</td>
              <td className="p-2 font-medium text-blue-700">{row.ngram}</td>
              
              {/* ✅ NEW: Normalized Score with visual emphasis */}
              <td className="p-2 bg-blue-50">
                <div className="flex items-center gap-2">
                  <span className="font-semibold text-blue-800">
                    {fmtNum(row.normalized_score)}
                  </span>
                  {/* Visual bar indicator */}
                  <div className="flex-1 bg-blue-200 rounded-full h-2 max-w-16">
                    <div 
                      className="bg-blue-600 h-2 rounded-full transition-all duration-300"
                      style={{ width: `${Math.max(2, row.normalized_score)}%` }}
                    />
                  </div>
                </div>
              </td>
              
              <td className="p-2 text-gray-700">{fmtScore(row.score)}</td>
              <td className="p-2">{row.subfield ?? "—"}</td>
              <td className="p-2">{row.field ?? "—"}</td>
              <td className="p-2">{row.domain ?? "—"}</td>
              <td className="p-2">{row.num_bursts ?? "—"}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {data.length === 0 && <div className="text-sm text-gray-500 p-2">No results.</div>}
    </div>
  );
};

export default BurstinessLeaderboard;