// frontend/src/utils/normalize.ts
export type XY = { date: string; count: number };

function safeDiv(a: number, b: number): number {
  return b === 0 ? 0 : a / b;
}

/**
 * Normalize each series by its own max → emphasizes shape per term
 * count_norm = count / max(count)
 */
export function normalizePerSeries(series: XY[]): XY[] {
  const max = Math.max(0, ...series.map(p => p.count));
  return series.map(p => ({ ...p, count: safeDiv(p.count, max) }));
}

/**
 * Normalize both series by the same max across the pair → preserves relative amplitudes
 * Useful if you want to keep a sense of "stronger burst" across the two.
 */
export function normalizePair(left: XY[], right: XY[]): [XY[], XY[]] {
  const maxL = Math.max(0, ...left.map(p => p.count));
  const maxR = Math.max(0, ...right.map(p => p.count));
  const maxBoth = Math.max(maxL, maxR, 0);
  return [
    left.map(p => ({ ...p, count: safeDiv(p.count, maxBoth) })),
    right.map(p => ({ ...p, count: safeDiv(p.count, maxBoth) })),
  ];
}

/**
 * Z-score normalization (optional). Centered, unit variance.
 * Better when you care about deviations from trend rather than absolute magnitude.
 */
export function zScore(series: XY[]): XY[] {
  const n = series.length || 1;
  const mean = series.reduce((acc, p) => acc + p.count, 0) / n;
  const variance = series.reduce((acc, p) => acc + (p.count - mean) ** 2, 0) / n;
  const std = Math.sqrt(variance) || 1;
  return series.map(p => ({ ...p, count: (p.count - mean) / std }));
}
