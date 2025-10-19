# app/burst_detection/kleinberg_burst_processor.py
import io
import json
import logging
import gc
from pathlib import Path
from typing import List, Dict, Tuple, Optional

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.models import BurstDetection
from .burst_algorithm import burst_detection, enumerate_bursts, burst_weights

logger = logging.getLogger("uvicorn")
logging.getLogger("numba").setLevel(logging.WARNING)
logging.getLogger("numba.core").setLevel(logging.WARNING)
logging.getLogger("numba.core.byteflow").setLevel(logging.ERROR)


class KleinbergBurstProcessor:
    """
    Kleinberg burst detection with FULL TIMELINE output.
    
    Outputs ALL time points for each ngram:
    - Contribution = burst weight where detected, 0 otherwise
    - Kleinberg state, probability for all points
    - Consistent timeline with MACD for visualization
    """

    def __init__(self, cache_dir: Path = None):
        self.cache_dir = cache_dir or Path("cache")
        self.cache_file = self.cache_dir / "processed_ngram_data.parquet"
        self.copy_chunk_rows = 50_000
        self.points_copy_chunk_rows = 500_000
        self.field_memory_threshold = 1000

    def has_cached_data(self) -> bool:
        return self.cache_file.exists() and self.cache_file.stat().st_size > 0

    def get_cache_info(self) -> dict:
        if not self.cache_file.exists():
            return {"exists": False}
        try:
            stat = self.cache_file.stat()
            df_sample = pd.read_parquet(self.cache_file, nrows=5)
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

        logger.info("📂 Loading cached data for Kleinberg burst detection...")
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

        sort_order = np.argsort(parsed_dates)
        time_cols = [valid_time_cols[i] for i in sort_order]
        time_index = pd.DatetimeIndex([parsed_dates[i] for i in sort_order])

        logger.info(f"📂 Time range: {time_index[0].date()} → {time_index[-1].date()} ({len(time_cols)} periods)")
        return df, time_cols, time_index

    def process_bursts_from_cache(self, db: Session, s: float = 5.7, gamma: float = 1.0):
        logger.info("🔥 Starting Kleinberg burst detection [FULL TIMELINE] ...")
        logger.info(f"🎛️ Parameters: s={s}, gamma={gamma}")
        logger.info("📊 Outputting ALL time points with burst weights (0 where no burst)")

        self._clear_existing_kleinberg_data(db)

        df, time_cols, time_index = self.load_cached_data()
        logger.info("📊 Calculating period totals...")
        total_per_period = df[time_cols].sum(axis=0).values.astype(float)
        logger.info(f"📊 Period totals: min={total_per_period.min():.0f}, max={total_per_period.max():.0f}")

        ngram_id_map = self._build_ngram_id_mapping(db)
        field_groups = self._group_by_field(df)
        logger.info(f"🗂️ Processing {len(field_groups)} field groups")

        total_processed = 0
        total_points = 0

        det_csv_buf = io.StringIO()
        det_rows_in_buf = 0

        pts_csv_buf = io.StringIO()
        pts_rows_in_buf = 0

        for i, ((domain, field), field_df) in enumerate(field_groups.items(), 1):
            logger.info(f"📄 [{i}/{len(field_groups)}] Processing: {domain} > {field} ({len(field_df):,} ngrams)")

            for _, row in field_df.iterrows():
                ngram_key = (row["n-gram"], row["domain"], row["field"], row["subfield"])
                ngram_id = ngram_id_map.get(ngram_key)
                if not ngram_id:
                    continue

                result = self._detect_bursts_for_ngram(
                    row, time_cols, total_per_period, ngram_id,
                    time_index=time_index, s=s, gamma=gamma
                )
                if not result:
                    continue

                intervals_json = json.dumps(result["burst_intervals"]).replace('"', '""')

                # Detection row
                det_csv_buf.write(
                    f'{result["ngram_id"]},"kleinberg",{result["global_score"]},,'
                    f'{result["num_bursts"]},"{intervals_json}",{s},{gamma},,,,\n'
                )
                det_rows_in_buf += 1

                # ✅ Points rows - ALL time points with complete metrics
                for pt in result["points"]:
                    # Format: ngram_id, method, date, period_index, contribution,
                    #         raw_value, baseline_value, macd_short_ema, macd_long_ema,
                    #         macd_line, macd_signal, macd_histogram,
                    #         kleinberg_state, state_probability, weight_contribution
                    pts_csv_buf.write(
                        f'{result["ngram_id"]},"kleinberg",{pt["date"]},{pt["period_index"]},'
                        f'{pt["contribution"]},{pt["raw_value"]},{pt["baseline_value"]},'
                        f',,,,,'  # Empty MACD fields (5 commas for 5 NULL columns)
                        f'{pt["kleinberg_state"] if pt["kleinberg_state"] is not None else ""},'
                        f'{pt["state_probability"] if pt["state_probability"] is not None else ""},'
                        f'{pt["weight_contribution"]}\n'
                    )
                    pts_rows_in_buf += 1
                
                total_points += len(result["points"])
                total_processed += 1

                # Occasional flush
                if det_rows_in_buf >= self.copy_chunk_rows:
                    self._flush_detections_buffer(db, det_csv_buf, det_rows_in_buf)
                    det_csv_buf = io.StringIO()
                    det_rows_in_buf = 0
                if pts_rows_in_buf >= self.points_copy_chunk_rows:
                    self._flush_points_copy(db, pts_csv_buf, pts_rows_in_buf)
                    pts_csv_buf = io.StringIO()
                    pts_rows_in_buf = 0

            logger.info(f"✅ Completed {domain} > {field}")

            if i % 5 == 0:
                self._memory_cleanup()

        # Final flush
        if det_rows_in_buf > 0:
            self._flush_detections_buffer(db, det_csv_buf, det_rows_in_buf)
        if pts_rows_in_buf > 0:
            self._flush_points_copy(db, pts_csv_buf, pts_rows_in_buf)

        self._update_ranks(db)
        logger.info(f"✅ Kleinberg burst detection completed!")
        logger.info(f"📊 Total time points written: {total_points:,}")

    def _clear_existing_kleinberg_data(self, db: Session):
        logger.info("🧹 Clearing existing Kleinberg burst data...")
        db.execute(text("DELETE FROM burst_points WHERE method = 'kleinberg'"))
        db.execute(text("DELETE FROM burst_detections WHERE method = 'kleinberg'"))
        db.commit()

    def _group_by_field(self, df: pd.DataFrame) -> Dict[Tuple[str, str], pd.DataFrame]:
        lower_map = {c.lower(): c for c in df.columns}
        domain_col = lower_map.get("domain")
        field_col = lower_map.get("field")
        if not domain_col or not field_col:
            raise ValueError(
                f"Missing required columns 'domain'/'field'. Available: {list(df.columns)}"
            )
        return {
            (str(d), str(f)): grp
            for (d, f), grp in df.groupby([domain_col, field_col], sort=False)
        }

    def _detect_bursts_for_ngram(
        self,
        row: pd.Series,
        time_cols: List[str],
        total_per_period: np.ndarray,
        ngram_id: int,
        *,
        time_index: pd.DatetimeIndex,
        s: float,
        gamma: float,
    ) -> Optional[Dict]:
        """
        Detect bursts and return FULL timeline.
        Returns ALL time points with contribution=0 where no burst.
        """
        r = row[time_cols].values.astype(float)
        d = total_per_period
        n = len(time_cols)

        if r.sum() == 0 or np.any(np.isnan(r)):
            return None

        q, _, _, p, _ = burst_detection(r, d, n=n, s=s, gamma=gamma)
        if q is None or np.all(np.isnan(q)):
            return None

        bursts_df = enumerate_bursts(q, label="ngram")
        
        # ✅ Handle no bursts detected - still output full timeline
        if len(bursts_df) == 0:
            points_data = []
            for t_idx in range(n):
                points_data.append({
                    "period_index": t_idx,
                    "date": time_index[t_idx].date().isoformat(),
                    "contribution": 0.0,
                    "raw_value": float(r[t_idx]),
                    "baseline_value": float(d[t_idx]),
                    "kleinberg_state": int(q[t_idx]) if q[t_idx] is not None else 0,
                    "state_probability": float(p.get(1, 0.0)) if isinstance(p, dict) else 0.0,
                    "weight_contribution": 0.0,
                })
            
            return {
                "ngram_id": int(ngram_id),
                "global_score": 0.0,
                "num_bursts": 0,
                "burst_intervals": [],
                "points": points_data,
            }

        weighted = burst_weights(bursts_df, r, d, p)
        weighted["duration"] = (weighted["end"] - weighted["begin"] + 1).astype(int)
        significant = weighted[(weighted["duration"] >= 2) & (weighted["weight"] > 0)]
        
        # ✅ Handle no significant bursts - still output full timeline
        if len(significant) == 0:
            points_data = []
            for t_idx in range(n):
                points_data.append({
                    "period_index": t_idx,
                    "date": time_index[t_idx].date().isoformat(),
                    "contribution": 0.0,
                    "raw_value": float(r[t_idx]),
                    "baseline_value": float(d[t_idx]),
                    "kleinberg_state": int(q[t_idx]) if q[t_idx] is not None else 0,
                    "state_probability": float(p.get(1, 0.0)) if isinstance(p, dict) else 0.0,
                    "weight_contribution": 0.0,
                })
            
            return {
                "ngram_id": int(ngram_id),
                "global_score": 0.0,
                "num_bursts": 0,
                "burst_intervals": [],
                "points": points_data,
            }

        global_score = float(significant["weight"].sum())
        num_bursts = int(len(significant))
        interval_list = [
            [time_index[int(srow.begin)].date().isoformat(), time_index[int(srow.end)].date().isoformat()]
            for _, srow in significant.iterrows()
        ]

        # ✅ Calculate per-period weights using step-span trick
        rows = []
        for _, srow in significant.iterrows():
            b, e = int(srow.begin), int(srow.end)
            for t in range(b, e + 1):
                rows.append({"begin": t, "end": t})

        # Build dict of burst weights by time index
        burst_weights_dict = {}
        if rows:
            step_df = pd.DataFrame(rows)
            step_weighted = burst_weights(step_df, r, d, p)
            for _, srow in step_weighted.iterrows():
                t_idx = int(srow["begin"])
                burst_weights_dict[t_idx] = float(srow["weight"])

        # ✅ Create ALL time points with weights (0 where not in burst)
        points_data = []
        for t_idx in range(n):
            contribution = burst_weights_dict.get(t_idx, 0.0)
            
            points_data.append({
                "period_index": t_idx,
                "date": time_index[t_idx].date().isoformat(),
                "contribution": contribution,
                "raw_value": float(r[t_idx]),
                "baseline_value": float(d[t_idx]),
                "kleinberg_state": int(q[t_idx]) if q[t_idx] is not None else 0,
                "state_probability": float(p.get(1, 0.0)) if isinstance(p, dict) else 0.0,
                "weight_contribution": contribution,
            })

        return {
            "ngram_id": int(ngram_id),
            "global_score": global_score,
            "num_bursts": num_bursts,
            "burst_intervals": interval_list,
            "points": points_data,
        }

    def _flush_detections_buffer(self, db: Session, csv_buffer: io.StringIO, rows_count: int):
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
        except Exception as e:
            raw_conn.rollback()
            logger.error(f"❌ Kleinberg detection flush failed: {e}")
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
            # ✅ Updated COPY statement to match new schema with all MACD fields
            cursor.copy_expert(
                """
                COPY burst_points
                (ngram_id, method, date, period_index, contribution,
                 raw_value, baseline_value, macd_short_ema, macd_long_ema,
                 macd_line, macd_signal, macd_histogram,
                 kleinberg_state, state_probability, weight_contribution)
                FROM STDIN WITH (FORMAT CSV, NULL '')
                """,
                csv_buffer,
            )
            raw_conn.commit()
        except Exception as e:
            raw_conn.rollback()
            logger.error(f"❌ Kleinberg points COPY failed: {e}")
            raise
        finally:
            cursor.close()

    def _build_ngram_id_mapping(self, db: Session) -> Dict:
        result = db.execute(text("""
            SELECT n.id, n.text, d.name as domain, f.name as field, s.name as subfield
            FROM ngrams n
            JOIN subfields s ON n.subfield_id = s.id
            JOIN fields f ON s.field_id = f.id
            JOIN domains d ON f.domain_id = d.id
        """))
        return {(row.text, row.domain, row.field, row.subfield): row.id for row in result}

    def _update_ranks(self, db: Session):
        db.execute(text("""
            UPDATE burst_detections
            SET rank = ranked.rank
            FROM (
                SELECT id, ROW_NUMBER() OVER (ORDER BY global_score DESC) AS rank
                FROM burst_detections
                WHERE method = 'kleinberg' AND global_score > 0
            ) ranked
            WHERE burst_detections.id = ranked.id
        """))
        db.commit()

    def _memory_cleanup(self):
        gc.collect()