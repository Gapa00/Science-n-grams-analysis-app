// src/components/BurstContributionChart.tsx
import {
  ComposedChart,
  Bar,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
  ReferenceArea,
  Legend,
} from "recharts";
import { BurstPoint } from "../api/client";

type Props = {
  data: BurstPoint[];
  method: "macd" | "kleinberg";
  ngramText?: string;
  hideTitle?: boolean;
  showTooltip?: boolean;
  xLabel?: string;
  yLabel?: string;
  subtitle?: React.ReactNode;
  height?: number;
  showAxes?: boolean;
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

function buildYearTicks(rows: { date: string }[]) {
  if (!rows.length) return [];
  const byYearFirstSeen = new Map<string, string>();
  const jan1Candidates = new Map<string, string>();

  for (const p of rows) {
    const y = p.date.slice(0, 4);
    if (!byYearFirstSeen.has(y)) byYearFirstSeen.set(y, p.date);
    const jan1 = `${y}-01-01`;
    if (p.date === jan1) jan1Candidates.set(y, jan1);
  }

  const years = Array.from(byYearFirstSeen.keys()).sort();
  return years.map((y) => jan1Candidates.get(y) ?? byYearFirstSeen.get(y)!);
}

const isoLTE = (a: string, b: string) => a <= b;
const isoLT = (a: string, b: string) => a < b;
const isoGTE = (a: string, b: string) => a >= b;

function BurstContributionChart({
  data,
  method,
  ngramText,
  hideTitle,
  showTooltip = true,
  xLabel = "Year",
  yLabel = "Burst Contribution",
  subtitle,
  height = 320,
  showAxes = true,
  intervalStart = null,
  intervalEnd = null,
}: Props) {
  if (!data || data.length === 0) {
    return (
      <div className="bg-white rounded-xl shadow p-4 mt-2">
        <div className="text-sm text-gray-500">No burst data available.</div>
      </div>
    );
  }

  // Prepare chart data
  const chartData = data.map((point) => ({
    date: point.date,
    contribution: point.contribution,
    raw_value: point.raw_value,
    baseline_value: point.baseline_value || 0,
    // Method-specific metrics
    macd_histogram: point.macd_histogram || 0,
    kleinberg_state: point.kleinberg_state || 0,
    state_probability: point.state_probability || 0,
  }));

  const dates = chartData.map((d) => d.date);
  const domainMin = dates.reduce((min, d) => (min < d ? min : d), dates[0]);
  const domainMax = dates.reduce((max, d) => (max > d ? max : d), dates[0]);

  // Compute shaded interval
  let included: string[] = [];
  if (intervalStart || intervalEnd) {
    included = dates.filter((d) => {
      const afterStart = intervalStart ? isoGTE(d, intervalStart) : true;
      const beforeEnd = intervalEnd ? isoLT(d, intervalEnd) : true;
      return afterStart && beforeEnd;
    });
  }

  const showBand = included.length > 0;
  const x1 = showBand ? included[0] : undefined;
  const x2 = showBand ? included[included.length - 1] : undefined;

  const yearTicks = buildYearTicks(chartData);

  // Determine what to show based on method
  const showMacdLine = method === "macd";
  const showKleinbergLine = method === "kleinberg";

  return (
    <div className="bg-white rounded-xl shadow p-4 mt-2">
      {!hideTitle && (
        <>
          <h2 className="text-sm sm:text-base font-semibold">
            Burst Detection Contributions{" "}
            <span className="text-xs text-gray-500">({method.toUpperCase()})</span>
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
        <ComposedChart data={chartData}>
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
                yAxisId="left"
                tick={{ fontSize: 11 }}
                label={{
                  value: yLabel,
                  angle: -90,
                  position: "insideLeft",
                  offset: 8,
                  fontSize: 11,
                }}
              />
              {showMacdLine && (
                <YAxis
                  yAxisId="right"
                  orientation="right"
                  tick={{ fontSize: 11 }}
                  label={{
                    value: "MACD Histogram",
                    angle: 90,
                    position: "insideRight",
                    offset: 8,
                    fontSize: 11,
                  }}
                />
              )}
              {showKleinbergLine && (
                <YAxis
                  yAxisId="right"
                  orientation="right"
                  tick={{ fontSize: 11 }}
                  label={{
                    value: "Burst State",
                    angle: 90,
                    position: "insideRight",
                    offset: 8,
                    fontSize: 11,
                  }}
                />
              )}
            </>
          )}

          {showTooltip && (
            <Tooltip
              formatter={(value: any, name: string) => {
                if (name === "contribution") return [fmtNum(value as number), "Contribution"];
                if (name === "macd_histogram") return [fmtNum(value as number), "MACD Histogram"];
                if (name === "kleinberg_state") return [value, "State Level"];
                if (name === "raw_value") return [value, "Raw Count"];
                if (name === "baseline_value") return [fmtNum(value as number), "Baseline"];
                return [value, name];
              }}
              labelFormatter={(label: any) => `Date: ${fmtDate(label as string)}`}
            />
          )}

          <Legend
            verticalAlign="top"
            height={36}
            iconType="line"
            wrapperStyle={{ fontSize: "12px" }}
          />

          {/* Contribution bars (primary metric) */}
          <Bar
            yAxisId="left"
            dataKey="contribution"
            fill="#3B82F6"
            name="Contribution"
            opacity={0.8}
          />

          {/* Method-specific overlay */}
          {showMacdLine && (
            <Line
              yAxisId="right"
              type="monotone"
              dataKey="macd_histogram"
              stroke="#EF4444"
              strokeWidth={2}
              dot={false}
              name="MACD Histogram"
            />
          )}

          {showKleinbergLine && (
            <Line
              yAxisId="right"
              type="stepAfter"
              dataKey="kleinberg_state"
              stroke="#10B981"
              strokeWidth={2}
              dot={{ r: 2 }}
              name="Burst State"
            />
          )}
        </ComposedChart>
      </ResponsiveContainer>

      {/* Legend explanation */}
      <div className="mt-3 text-xs text-gray-600 space-y-1">
        <div>
          <strong>Contribution:</strong> Burst score contribution at each time point{" "}
          {method === "macd" && "(Positive MACD histogram / √denominator)"}
          {method === "kleinberg" && "(Kleinberg burst weight)"}
        </div>
        {method === "macd" && (
          <div>
            <strong>MACD Histogram:</strong> MACD line minus Signal line (raw values before normalization)
          </div>
        )}
        {method === "kleinberg" && (
          <div>
            <strong>Burst State:</strong> State level (0 = baseline, 1+ = burst intensity)
          </div>
        )}
      </div>
    </div>
  );
}

export default BurstContributionChart;