# app/burst_detection/macd_burst_processor.py
import io
import json
import logging
import gc
import numpy as np
import pandas as pd
from pathlib import Path
from typing import List, Dict, Tuple
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.models import BurstDetection

logger = logging.getLogger("uvicorn")

class MacdBurstProcessor:
    """
    MACD-based burst detection with FULL TIMELINE output.
    
    Outputs ALL time points for each ngram with complete MACD metrics:
    - Short EMA, Long EMA (baseline), MACD line, Signal, Histogram
    - Contribution (can be negative)
    - Raw values
    
    This allows consistent visualization across entire timeline.
    """

    def __init__(self, cache_dir: Path = None):
        self.cache_dir = cache_dir or Path("cache")
        self.cache_file = self.cache_dir / "processed_ngram_data.parquet"

        # I/O buffers
        self.copy_chunk_rows = 200_000
        self.points_copy_chunk_rows = 500_000

        # MACD params (quarters)
        self.short_span = 6
        self.long_span = 16
        self.signal_span = 7

        # For detection table only
        self.poisson_threshold = 2.0

        # Eq. (5) denominator mode
        self.eq5_denom_mode = "sqrt_pctl"
        self.eq5_denom_percentile = 99.5

        # Chunking for CPU/mem balance
        self.chunk_size = 4000

        logger.debug(f"Initialized MacdBurstProcessor with cache_dir: {self.cache_dir}")

    def has_cached_data(self) -> bool:
        return self.cache_file.exists() and self.cache_file.stat().st_size > 0

    def get_cache_info(self) -> dict:
        if not self.cache_file.exists():
            return {"exists": False}
        try:
            stat = self.cache_file.stat()
            df_sample = pd.read_parquet(self.cache_file).head(5)
            return {
                "exists": True,
                "size_mb": round(stat.st_size / (1024 * 1024), 2),
                "columns": len(df_sample.columns),
                "sample_data": len(df_sample) > 0
            }
        except Exception as e:
            return {"exists": True, "error": str(e)}

    def load_cached_data(self) -> Tuple[pd.DataFrame, List[str], pd.DatetimeIndex]:
        if not self.has_cached_data():
            raise FileNotFoundError(f"No cached data at {self.cache_file}. Run preprocessing first.")
        logger.info("📂 Loading cached data for MACD burst detection...")
        df = pd.read_parquet(self.cache_file)
        logger.info(f"📂 Loaded {len(df):,} ngrams with {len(df.columns)} columns")

        non_time_cols = {
            "n-gram", "domain", "field", "subfield", "Subfield",
            "DF(n-gram)", "DF(n-gram, subfield)", "n_words",
        }
        time_candidates = [c for c in df.columns if c not in non_time_cols]

        valid_time_cols, parsed_dates = [], []
        for col in time_candidates:
            try:
                date_str = str(col)[:10]
                parsed_dates.append(pd.to_datetime(date_str, format="%Y-%m-%d"))
                valid_time_cols.append(col)
            except (ValueError, TypeError):
                continue

        if not valid_time_cols:
            raise ValueError("No valid time columns found in cached data")

        order = np.argsort(parsed_dates)
        time_cols = [valid_time_cols[i] for i in order]
        time_index = pd.DatetimeIndex([parsed_dates[i] for i in order])
        logger.info(f"📂 Time range: {time_index[0].date()} → {time_index[-1].date()} ({len(time_cols)} periods)")
        return df, time_cols, time_index

    def process_bursts_from_cache(self, db: Session):
        logger.info("🔥 Starting MACD burst detection [FULL TIMELINE] ...")
        logger.info(f"🎛️ MACD params: short={self.short_span}, long={self.long_span}, signal={self.signal_span}")
        logger.info(f"🧮 Denominator: {self.eq5_denom_mode} (pctl={self.eq5_denom_percentile if self.eq5_denom_mode=='sqrt_pctl' else '—'})")
        logger.info("📊 Outputting ALL time points with complete MACD metrics")

        self._clear_existing_macd_data(db)

        df, time_cols, time_index = self.load_cached_data()
        ngram_id_map = self._build_ngram_id_mapping(db)
        field_groups = self._group_by_field(df)

        det_csv_buf = io.StringIO()
        det_rows_in_buf = 0

        pts_csv_buf = io.StringIO()
        pts_rows_in_buf = 0

        total_burst_points = 0

        for i, ((domain, field), field_df) in enumerate(field_groups.items(), 1):
            logger.info(f"📄 [{i}/{len(field_groups)}] {domain} > {field} ({len(field_df):,} ngrams)")

            for start in range(0, len(field_df), self.chunk_size):
                chunk = field_df.iloc[start:start + self.chunk_size]
                results = self._calculate_macd_metrics_vectorized(chunk, time_cols)

                keys = list(zip(chunk["n-gram"], chunk["domain"], chunk["field"], chunk["subfield"]))
                ngram_ids = [ngram_id_map.get(k) for k in keys]

                for ngram_id, res in zip(ngram_ids, results):
                    if not ngram_id or res is None:
                        continue

                    # Detection row
                    intervals_json = json.dumps(res["burst_intervals"]).replace('"', '""')
                    det_csv_buf.write(
                        f'{int(ngram_id)},"macd",{res["global_score"]},,'
                        f'{res["num_bursts"]},"{intervals_json}",,,'
                        f'{self.short_span},{self.long_span},{self.signal_span},{self.poisson_threshold}\n'
                    )
                    det_rows_in_buf += 1

                    # ✅ Points rows - ALL time points with complete metrics
                    for p in res["points"]:
                        pts_csv_buf.write(
                            f'{int(ngram_id)},"macd",{p["date"]},{p["period_index"]},'
                            f'{p["contribution"]},{p["raw_value"]},{p["baseline_value"]},'
                            f'{p["macd_short_ema"]},{p["macd_long_ema"]},{p["macd_line"]},'
                            f'{p["macd_signal"]},{p["macd_histogram"]}\n'
                        )
                        pts_rows_in_buf += 1

                    total_burst_points += len(res["points"])

                    if det_rows_in_buf >= self.copy_chunk_rows:
                        self._flush_detections_copy(db, det_csv_buf, det_rows_in_buf)
                        det_csv_buf = io.StringIO()
                        det_rows_in_buf = 0
                    if pts_rows_in_buf >= self.points_copy_chunk_rows:
                        self._flush_points_copy(db, pts_csv_buf, pts_rows_in_buf)
                        pts_csv_buf = io.StringIO()
                        pts_rows_in_buf = 0

            logger.info(f"✅ Completed {domain} > {field}")
            self._memory_cleanup()

        if det_rows_in_buf > 0:
            self._flush_detections_copy(db, det_csv_buf, det_rows_in_buf)
        if pts_rows_in_buf > 0:
            self._flush_points_copy(db, pts_csv_buf, pts_rows_in_buf)

        self._update_ranks(db)
        logger.info("✅ MACD burst detection completed!")
        logger.info(f"📊 Total time points written: {total_burst_points:,}")

    def _calculate_macd_metrics_vectorized(self, chunk: pd.DataFrame, time_cols: List[str]):
        """
        Calculate MACD metrics for ALL time points.
        Returns complete timeline with all MACD values.
        """
        P = chunk[time_cols].fillna(0.0).to_numpy(dtype=float)
        if P.size == 0:
            return []

        # Compute MACD components
        dfP = pd.DataFrame(P)
        short_ema = dfP.T.ewm(alpha=2/(self.short_span+1), adjust=False).mean().T.to_numpy()
        long_ema  = dfP.T.ewm(alpha=2/(self.long_span+1),  adjust=False).mean().T.to_numpy()
        macd_line = short_ema - long_ema
        signal    = pd.DataFrame(macd_line).T.ewm(alpha=2/(self.signal_span+1), adjust=False).mean().T.to_numpy()
        histogram = macd_line - signal

        # Vectorized denominator
        if self.eq5_denom_mode == "sqrt_pctl":
            Ppos = np.where(P > 0, P, np.nan)
            base = np.nanpercentile(Ppos, self.eq5_denom_percentile, axis=1)
            row_max = np.max(P, axis=1)
            base = np.where(np.isfinite(base) & (base > 0), base, row_max)
        else:
            base = np.max(P, axis=1)

        eps = 1e-12
        base = np.where((base > 0) & np.isfinite(base), base, eps)
        denom = np.sqrt(base)[:, None]

        # ✅ Contributions for ALL points (can be negative)
        contrib = histogram / denom

        n_rows, n_cols = P.shape
        results = [None] * n_rows
        time_strs = [str(c)[:10] for c in time_cols]

        for r in range(n_rows):
            # Only consider positive contributions for burst detection
            positive_mask = histogram[r, :] > 0
            
            # Global score: sum of positive contributions only
            global_score = float(contrib[r, positive_mask].sum()) if positive_mask.any() else 0.0

            # Burst intervals: consecutive positive histogram regions
            intervals = []
            in_burst = False
            start_idx = None
            for c in range(n_cols):
                if positive_mask[c] and not in_burst:
                    in_burst = True
                    start_idx = c
                elif not positive_mask[c] and in_burst:
                    in_burst = False
                    if start_idx is not None:
                        intervals.append([time_strs[start_idx], time_strs[c-1]])
            if in_burst and start_idx is not None:
                intervals.append([time_strs[start_idx], time_strs[-1]])

            # ✅ Output ALL points with complete MACD metrics
            points = []
            for c in range(n_cols):
                points.append({
                    "period_index": int(c),
                    "date": time_strs[c],
                    "contribution": float(contrib[r, c]),
                    "raw_value": float(P[r, c]),
                    "baseline_value": float(long_ema[r, c]),
                    "macd_short_ema": float(short_ema[r, c]),
                    "macd_long_ema": float(long_ema[r, c]),
                    "macd_line": float(macd_line[r, c]),
                    "macd_signal": float(signal[r, c]),
                    "macd_histogram": float(histogram[r, c]),
                })

            results[r] = {
                "global_score": global_score,
                "num_bursts": len(intervals),
                "burst_intervals": intervals,
                "points": points,
            }

        return results

    def _clear_existing_macd_data(self, db: Session):
        logger.info("🧹 Clearing existing MACD burst data...")
        points_deleted = db.execute(text("DELETE FROM burst_points WHERE method = 'macd'")).rowcount
        detections_deleted = db.execute(text("DELETE FROM burst_detections WHERE method = 'macd'")).rowcount
        db.commit()
        logger.info(f"🧹 Cleared {points_deleted:,} MACD points and {detections_deleted:,} MACD detections")

    def _flush_detections_copy(self, db: Session, csv_buffer: io.StringIO, rows_count: int):
        if rows_count == 0:
            return
        csv_buffer.seek(0)
        raw_conn = db.connection().connection
        cursor = raw_conn.cursor()
        try:
            cursor.copy_expert(
                """
                COPY burst_detections
                (ngram_id, method, global_score, rank, num_bursts, burst_intervals,
                 kleinberg_s_param, kleinberg_gamma_param, macd_short_span,
                 macd_long_span, macd_signal_span, poisson_threshold)
                FROM STDIN WITH (FORMAT CSV, NULL '')
                """,
                csv_buffer,
            )
            raw_conn.commit()
            logger.debug(f"💾 Flushed {rows_count:,} detection rows via COPY")
        except Exception as e:
            raw_conn.rollback()
            logger.error(f"❌ Detection COPY failed: {e}")
            raise
        finally:
            cursor.close()

    def _flush_points_copy(self, db: Session, csv_buffer: io.StringIO, rows_count: int):
        if rows_count == 0:
            return
        csv_buffer.seek(0)
        raw_conn = db.connection().connection
        cursor = raw_conn.cursor()
        try:
            cursor.copy_expert(
                """
                COPY burst_points
                (ngram_id, method, date, period_index, contribution,
                 raw_value, baseline_value, macd_short_ema, macd_long_ema, 
                 macd_line, macd_signal, macd_histogram)
                FROM STDIN WITH (FORMAT CSV, NULL '')
                """,
                csv_buffer,
            )
            raw_conn.commit()
            logger.debug(f"💾 Flushed {rows_count:,} point rows via COPY")
        except Exception as e:
            raw_conn.rollback()
            logger.error(f"❌ Points COPY failed: {e}")
            raise
        finally:
            cursor.close()

    def _build_ngram_id_mapping(self, db: Session) -> Dict:
        logger.info("🔗 Building ngram ID mapping...")
        result = db.execute(text("""
            SELECT n.id, n.text, d.name as domain, f.name as field, s.name as subfield
            FROM ngrams n
            JOIN subfields s ON n.subfield_id = s.id
            JOIN fields f ON s.field_id = f.id
            JOIN domains d ON f.domain_id = d.id
        """))
        ngram_map = {(row.text, row.domain, row.field, row.subfield): row.id for row in result}
        logger.info(f"🔗 Built mapping for {len(ngram_map):,} ngrams")
        return ngram_map

    def _group_by_field(self, df: pd.DataFrame) -> Dict[Tuple[str, str], pd.DataFrame]:
        try:
            lower_map = {c.lower(): c for c in df.columns}
            domain_col = lower_map.get("domain")
            field_col = lower_map.get("field")
            if not domain_col or not field_col:
                raise ValueError("Missing required columns 'domain'/'field'.")
            field_groups: Dict[Tuple[str, str], pd.DataFrame] = {}
            for (domain, field), grp in df.groupby([domain_col, field_col], sort=False):
                key = (str(domain), str(field))
                field_groups[key] = grp
            return dict(sorted(field_groups.items(), key=lambda kv: len(kv[1]), reverse=True))
        except Exception as e:
            import traceback
            logger.error(f"❌ _group_by_field failed: {e}")
            logger.error(traceback.format_exc())
            return {}

    def _update_ranks(self, db: Session):
        logger.info("🏆 Calculating MACD global ranks...")
        db.execute(text("""
            UPDATE burst_detections
               SET rank = ranked.rank
              FROM (
                    SELECT id, ROW_NUMBER() OVER (ORDER BY global_score DESC) AS rank
                      FROM burst_detections
                     WHERE method = 'macd' AND global_score > 0
              ) ranked
             WHERE burst_detections.id = ranked.id
        """))
        db.commit()
        logger.info("✅ MACD ranks updated successfully")

    def _memory_cleanup(self):
        collected = gc.collect()
        logger.debug(f"🧹 Garbage collected {collected} objects")