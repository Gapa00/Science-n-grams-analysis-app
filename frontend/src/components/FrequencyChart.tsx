// src/components/FrequencyChart.tsx
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
  ReferenceArea,
} from "recharts";

type Props = {
  data: { date: string; count: number }[];
  ngramText?: string;
  hideTitle?: boolean;
  showTooltip?: boolean;
  xLabel?: string;
  yLabel?: string;
  subtitle?: React.ReactNode;
  height?: number;
  showAxes?: boolean;  // Control for showing axes
  noWrap?: boolean;    // Skip the wrapper div styling
  /** Optional shaded window on the X-axis (ISO dates: YYYY-MM-DD), end-exclusive */
  intervalStart?: string | null;
  intervalEnd?: string | null;
};

const fmtYear = (d: string) => (d?.length >= 4 ? d.slice(0, 4) : d);
const fmtDate = (d: any) => {
  if (typeof d === "string" && d) return d.slice(0, 10);
  return String(d || "");
};
const fmtNum = (n: number) =>
  new Intl.NumberFormat(undefined, { maximumFractionDigits: 3 }).format(n);

// Return one tick per year, aligned to Jan 1 when present;
// otherwise use the first data point we have for that year.
function buildYearTicks(rows: { date: string }[]) {
  if (!rows.length) return [];
  const byYearFirstSeen = new Map<string, string>(); // year -> first seen date
  const jan1Candidates = new Map<string, string>();  // year -> YYYY-01-01 (if exists)

  for (const p of rows) {
    const y = p.date.slice(0, 4);
    if (!byYearFirstSeen.has(y)) byYearFirstSeen.set(y, p.date);
    const jan1 = `${y}-01-01`;
    if (p.date === jan1) jan1Candidates.set(y, jan1);
  }

  const years = Array.from(byYearFirstSeen.keys()).sort();
  return years.map((y) => jan1Candidates.get(y) ?? byYearFirstSeen.get(y)!);
}

// Helper: strict ISO date compare for "YYYY-MM-DD" strings
const isoLTE = (a: string, b: string) => a <= b;
const isoLT = (a: string, b: string) => a < b;
const isoGTE = (a: string, b: string) => a >= b;
const isoGT = (a: string, b: string) => a > b;

function FrequencyChart({
  data,
  ngramText,
  hideTitle,
  showTooltip = false,
  xLabel = "Year",
  yLabel = "Frequency",
  subtitle,
  height = 260,
  showAxes = false,
  noWrap = false,
  intervalStart = null,
  intervalEnd = null,
}: Props) {
  if (!data || data.length === 0) return null;

  // Data is categorical by quarter (e.g., 2022-01-01, 2022-04-01, ...).
  // Derive domain from data (ISO strings compare lexicographically).
  const dates = data.map((d) => d.date);
  const domainMin = dates.reduce((min, d) => (min < d ? min : d), dates[0]);
  const domainMax = dates.reduce((max, d) => (max > d ? max : d), dates[0]);

  // Compute the *included quarters* for the [start, end) window on a categorical axis.
  // We shade only the quarters that satisfy: (start ? d >= start : true) AND (end ? d < end : true).
  let included: string[] = [];
  if (intervalStart || intervalEnd) {
    included = dates.filter((d) => {
      const afterStart = intervalStart ? isoGTE(d, intervalStart) : true;
      const beforeEnd = intervalEnd ? isoLT(d, intervalEnd) : true; // end-exclusive
      return afterStart && beforeEnd;
    });
  }

  // Determine shaded band bounds as the first and last *included* category.
  // If the window produces no matching quarters, don't show the band.
  const showBand = included.length > 0;
  // Clamp x1/x2 to domain by using actual data categories inside the window.
  const x1 = showBand ? included[0] : undefined;
  const x2 = showBand ? included[included.length - 1] : undefined;

  // Year-aligned ticks (keeps the quarterly axis readable)
  const yearTicks = buildYearTicks(data);

  const chartContent = (
    <>
      {!hideTitle && (
        <>
          <h2 className="text-sm sm:text-base font-semibold">
            Frequency over Time
            {ngramText ? (
              <>
                : <span className="text-blue-600">{ngramText}</span>
              </>
            ) : null}
          </h2>
          {subtitle && <div className="text-xs text-gray-500 mt-1">{subtitle}</div>}
          <div className="h-2" />
        </>
      )}
      <ResponsiveContainer width="100%" height={height}>
        <LineChart data={data}>
          <CartesianGrid strokeDasharray="3 3" />

          {showBand && x1 && x2 && (
            <ReferenceArea
              x1={x1}
              x2={x2}
              strokeOpacity={0}
              fill="#BFDBFE"
              fillOpacity={0.35}
            />
          )}

          {showAxes && (
            <>
              <XAxis
                dataKey="date"
                type="category"
                allowDuplicatedCategory={false}
                ticks={yearTicks}
                tick={{ fontSize: 11 }}
                tickFormatter={fmtYear}
                minTickGap={12}
                tickMargin={6}
                label={{
                  value: xLabel,
                  position: "insideBottomRight",
                  offset: -4,
                  fontSize: 11,
                }}
              />
              <YAxis
                tick={{ fontSize: 11 }}
                label={{
                  value: yLabel,
                  angle: -90,
                  position: "insideLeft",
                  offset: 8,
                  fontSize: 11,
                }}
              />
            </>
          )}

          {showTooltip && (
            <Tooltip
              formatter={(value: any) => [fmtNum(value as number), "Frequency"]}
              labelFormatter={(label: any) => `Date: ${fmtDate(label as string)}`}
            />
          )}

          <Line type="monotone" dataKey="count" stroke="#2563eb" strokeWidth={2} dot={false} />
        </LineChart>
      </ResponsiveContainer>
    </>
  );

  if (noWrap) return chartContent as any;

  return <div className="bg-white rounded-xl shadow p-4 mt-2">{chartContent}</div>;
}

export default FrequencyChart;
