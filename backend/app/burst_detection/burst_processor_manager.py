# app/burst_detection/burst_processor_manager.py
import logging
import os
from pathlib import Path
from typing import Dict, Any, Optional
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.burst_detection.kleinberg_burst_processor import KleinbergBurstProcessor
from app.burst_detection.macd_burst_processor import MacdBurstProcessor

logger = logging.getLogger("uvicorn")


class BurstProcessorManager:
    """
    Manages both Kleinberg and MACD burst detection processors.
    Provides unified interface for running burst detection analysis.
    """

    def __init__(self, cache_dir: Path = None):
        self.cache_dir = cache_dir or Path("cache")
        self.kleinberg_processor = KleinbergBurstProcessor(cache_dir)
        self.macd_processor = MacdBurstProcessor(cache_dir)

    # ---------- Cache ----------
    def has_cached_data(self) -> bool:
        """Check if cached data exists for processing."""
        return self.kleinberg_processor.has_cached_data()

    def get_cache_info(self) -> Dict[str, Any]:
        """Get information about cached data."""
        return self.kleinberg_processor.get_cache_info()

    # ---------- Detection Status ----------
    def get_detection_status(self, db: Session) -> Dict[str, Any]:
        """Get current status of burst detections in the database."""
        try:
            kleinberg_count = db.execute(text(
                "SELECT COUNT(*) FROM burst_detections WHERE method = 'kleinberg'"
            )).scalar() or 0

            macd_count = db.execute(text(
                "SELECT COUNT(*) FROM burst_detections WHERE method = 'macd'"
            )).scalar() or 0

            points_count = db.execute(text(
                "SELECT COUNT(*) FROM burst_points"
            )).scalar() or 0

            kleinberg_points = db.execute(text(
                "SELECT COUNT(*) FROM burst_points WHERE method = 'kleinberg'"
            )).scalar() or 0

            macd_points = db.execute(text(
                "SELECT COUNT(*) FROM burst_points WHERE method = 'macd'"
            )).scalar() or 0

            return {
                "kleinberg_detections": kleinberg_count,
                "macd_detections": macd_count,
                "total_detections": kleinberg_count + macd_count,
                "total_points": points_count,
                "kleinberg_points": kleinberg_points,
                "macd_points": macd_points,
                "has_kleinberg": kleinberg_count > 0,
                "has_macd": macd_count > 0,
            }
        except Exception as e:
            logger.error(f"Failed to get detection status: {e}")
            return {
                "kleinberg_detections": 0,
                "macd_detections": 0,
                "total_detections": 0,
                "total_points": 0,
                "kleinberg_points": 0,
                "macd_points": 0,
                "has_kleinberg": False,
                "has_macd": False,
                "error": str(e),
            }

    # ---------- Run Detectors ----------
    def run_kleinberg_detection(
        self,
        db: Session,
        s: float = 5.7,
        gamma: float = 1.0,
        force_rerun: bool = False,
    ) -> bool:
        """Run Kleinberg burst detection."""
        try:
            status = self.get_detection_status(db)

            if status["has_kleinberg"] and not force_rerun:
                logger.info(f"✅ Kleinberg detection already exists ({status['kleinberg_detections']:,} records)")
                return True

            if not self.has_cached_data():
                logger.error("❌ No cached data found for Kleinberg detection")
                return False

            logger.info("🔥 Starting Kleinberg burst detection...")
            self.kleinberg_processor.process_bursts_from_cache(db, s=s, gamma=gamma)

            if os.getenv("DISABLE_LEADERBOARD_EXPORT") != "1":
                self._export_kleinberg_leaderboard(db)

            logger.info("✅ Kleinberg burst detection completed successfully")
            return True

        except Exception as e:
            logger.error(f"❌ Kleinberg burst detection failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def run_macd_detection(
        self,
        db: Session,
        force_rerun: bool = False,
    ) -> bool:
        """Run MACD burst detection."""
        try:
            status = self.get_detection_status(db)

            if status["has_macd"] and not force_rerun:
                logger.info(f"✅ MACD detection already exists ({status['macd_detections']:,} records)")
                return True

            if not self.has_cached_data():
                logger.error("❌ No cached data found for MACD detection")
                return False

            logger.info("🔥 Starting MACD burst detection...")
            self.macd_processor.process_bursts_from_cache(db)

            if os.getenv("DISABLE_LEADERBOARD_EXPORT") != "1":
                self._export_macd_leaderboard(db)

            logger.info("✅ MACD burst detection completed successfully")
            return True

        except Exception as e:
            logger.error(f"❌ MACD burst detection failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def run_both_methods(
        self,
        db: Session,
        kleinberg_params: Optional[Dict[str, float]] = None,
        force_rerun: bool = False,
    ) -> Dict[str, bool]:
        """Run both Kleinberg and MACD burst detection."""
        kleinberg_params = kleinberg_params or {"s": 5.7, "gamma": 1.0}
        results = {"kleinberg": False, "macd": False}

        logger.info("🚀 Starting burst detection for both methods...")

        results["kleinberg"] = self.run_kleinberg_detection(
            db,
            s=kleinberg_params.get("s", 5.7),
            gamma=kleinberg_params.get("gamma", 1.0),
            force_rerun=force_rerun,
        )

        results["macd"] = self.run_macd_detection(db, force_rerun=force_rerun)

        logger.info(f"🏁 Burst detection completed: {sum(results.values())}/2 methods successful")

        for method, success in results.items():
            logger.info(f"  {'✅' if success else '❌'} {method.capitalize()}")

        return results

    # ---------- Export (streaming, O(1) RAM) ----------
    def _export_kleinberg_leaderboard(self, db: Session):
        """Export Kleinberg leaderboard to CSV via server-side COPY (memory-safe)."""
        try:
            logger.info("💾 Building Kleinberg leaderboard via server-side COPY (streaming)...")
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            csv_path = self.cache_dir / "kleinberg_leaderboard.csv"

            copy_sql = """
                COPY (
                    SELECT
                        bd.id               AS detection_id,
                        bd.ngram_id         AS ngram_id,
                        n.text              AS ngram,
                        d.name              AS domain,
                        f.name              AS field,
                        s.name              AS subfield,
                        bd.global_score     AS global_score,
                        bd.rank             AS rank,
                        bd.num_bursts       AS num_bursts,
                        bd.burst_intervals  AS burst_intervals,
                        bd.kleinberg_s_param AS s_param,
                        bd.kleinberg_gamma_param AS gamma_param
                    FROM burst_detections bd
                    JOIN ngrams n     ON bd.ngram_id = n.id
                    JOIN subfields s  ON n.subfield_id = s.id
                    JOIN fields f     ON s.field_id = f.id
                    JOIN domains d    ON f.domain_id = d.id
                    WHERE bd.method = 'kleinberg'
                    ORDER BY COALESCE(bd.rank, 2147483647), bd.global_score DESC
                ) TO STDOUT WITH (FORMAT CSV, HEADER, NULL '');
            """

            raw_conn = db.connection().connection
            cur = raw_conn.cursor()
            try:
                with open(csv_path, "w", newline="", encoding="utf-8") as f:
                    cur.copy_expert(copy_sql, f)
                raw_conn.commit()
                logger.info(f"✅ Saved Kleinberg CSV (streamed): {csv_path}")
            except Exception as e:
                raw_conn.rollback()
                logger.error(f"❌ Kleinberg leaderboard COPY failed: {e}")
                raise
            finally:
                cur.close()

        except Exception as e:
            logger.error(f"❌ Failed to export Kleinberg leaderboard (streaming): {e}")

    def _export_macd_leaderboard(self, db: Session):
        """Export MACD leaderboard to CSV via server-side COPY (memory-safe)."""
        try:
            logger.info("💾 Building MACD leaderboard via server-side COPY (streaming)...")
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            csv_path = self.cache_dir / "macd_leaderboard.csv"

            copy_sql = """
                COPY (
                    SELECT
                        bd.id               AS detection_id,
                        bd.ngram_id         AS ngram_id,
                        n.text              AS ngram,
                        d.name              AS domain,
                        f.name              AS field,
                        s.name              AS subfield,
                        bd.global_score     AS global_score,
                        bd.rank             AS rank,
                        bd.num_bursts       AS num_bursts,
                        bd.burst_intervals  AS burst_intervals,
                        bd.macd_short_span  AS short_span,
                        bd.macd_long_span   AS long_span,
                        bd.macd_signal_span AS signal_span,
                        bd.poisson_threshold AS poisson_threshold
                    FROM burst_detections bd
                    JOIN ngrams n     ON bd.ngram_id = n.id
                    JOIN subfields s  ON n.subfield_id = s.id
                    JOIN fields f     ON s.field_id = f.id
                    JOIN domains d    ON f.domain_id = d.id
                    WHERE bd.method = 'macd'
                    ORDER BY COALESCE(bd.rank, 2147483647), bd.global_score DESC
                ) TO STDOUT WITH (FORMAT CSV, HEADER, NULL '');
            """

            raw_conn = db.connection().connection
            cur = raw_conn.cursor()
            try:
                with open(csv_path, "w", newline="", encoding="utf-8") as f:
                    cur.copy_expert(copy_sql, f)
                raw_conn.commit()
                logger.info(f"✅ Saved MACD CSV (streamed): {csv_path}")
            except Exception as e:
                raw_conn.rollback()
                logger.error(f"❌ MACD leaderboard COPY failed: {e}")
                raise
            finally:
                cur.close()

        except Exception as e:
            logger.error(f"❌ Failed to export MACD leaderboard (streaming): {e}")

    # ---------- Utilities ----------
    def clear_all_detections(self, db: Session):
        """Clear all burst detection data from the database."""
        logger.info("🧹 Clearing all burst detection data...")
        points_deleted = db.execute(text("DELETE FROM burst_points")).rowcount
        detections_deleted = db.execute(text("DELETE FROM burst_detections")).rowcount
        db.commit()
        logger.info(f"🧹 Cleared {points_deleted:,} points and {detections_deleted:,} detections")

    def get_method_summary(self, db: Session) -> Dict[str, Any]:
        """Get summary statistics for both detection methods."""
        try:
            summary = {}

            # Kleinberg summary
            kleinberg_stats = db.execute(text("""
                SELECT 
                    COUNT(*) AS total,
                    AVG(global_score) AS avg_score,
                    MAX(global_score) AS max_score,
                    MIN(global_score) AS min_score,
                    AVG(num_bursts) AS avg_bursts
                FROM burst_detections 
                WHERE method = 'kleinberg' AND global_score > 0
            """)).mappings().first()
            summary["kleinberg"] = dict(kleinberg_stats) if kleinberg_stats else {}

            # MACD summary
            macd_stats = db.execute(text("""
                SELECT 
                    COUNT(*) AS total,
                    AVG(global_score) AS avg_score,
                    MAX(global_score) AS max_score,
                    MIN(global_score) AS min_score,
                    AVG(num_bursts) AS avg_bursts
                FROM burst_detections 
                WHERE method = 'macd' AND global_score > 0
            """)).mappings().first()
            summary["macd"] = dict(macd_stats) if macd_stats else {}

            # Point-level summary
            point_stats = db.execute(text("""
                SELECT 
                    method,
                    COUNT(*) AS total_points,
                    AVG(contribution) AS avg_contribution,
                    MAX(contribution) AS max_contribution
                FROM burst_points
                GROUP BY method
            """)).mappings().all()

            for stat in point_stats:
                method = stat["method"]
                if method in summary:
                    summary[method]["point_stats"] = dict(stat)

            return summary

        except Exception as e:
            logger.error(f"Failed to get method summary: {e}")
            return {"error": str(e)}
