import json
import os
import logging
from datetime import datetime
from pathlib import Path
from sqlalchemy import text, inspect
from sqlalchemy.orm import Session

from app.core.database import engine
from app.models import Base
from app.preprocessing.fast_preprocessor import UltraFastPreprocessor
from app.preprocessing.loader import NgramFileLoader
from app.preprocessing.resolver import SubfieldHierarchyResolver
from app.burst_detection.burst_processor_manager import BurstProcessorManager

logger = logging.getLogger("uvicorn")

# ----------------- Helpers (module-level) -----------------

def _dedup_by_id(rows: list[dict]) -> list[dict]:
    """Return items with unique 'id' in original order."""
    seen: set = set()
    out: list[dict] = []
    for r in rows or []:
        rid = r.get("id") if isinstance(r, dict) else None
        if rid is None:
            continue
        if rid not in seen:
            seen.add(rid)
            out.append(r)
    return out

def _log_slider_stats(tag: str, rows: list[dict]) -> None:
    """Log simple stats about the slider list."""
    ids = [r.get("id") for r in rows if isinstance(r, dict)]
    unique = len(set(ids))
    # Only compute duplicates count cheaply; we don't list them all to avoid O(n^2) logging on big lists
    dups_count = 0
    seen = set()
    for rid in ids:
        if rid in seen:
            dups_count += 1
        else:
            seen.add(rid)
    logger.info(f"[slider:{tag}] total={len(rows)} unique_ids={unique} dups_count={dups_count} sample_ids={ids[:10]}")

# ---------------------------------------------------------


class ApplicationInitializer:
    """Handles application initialization including data loading and burst detection."""

    def __init__(self):
        self.burst_manager = BurstProcessorManager()

    def initialize_database(self, db: Session) -> dict:
        logger.info("🔍 Checking database initialization status...")
        try:
            logger.info("🛠️ Creating database schema...")
            Base.metadata.create_all(bind=engine)
            logger.info("✅ Database schema created successfully")

            # 🔧 Ensure the PG enum has both labels (add if missing), then introspect.
            try:
                db.execute(text("ALTER TYPE burstmethod ADD VALUE IF NOT EXISTS 'kleinberg'"))
                db.execute(text("ALTER TYPE burstmethod ADD VALUE IF NOT EXISTS 'macd'"))
                db.commit()
            except Exception as e:
                db.rollback()
                logger.debug(f"Enum ensure note: {e}")

            # ✅ Safe introspection (doesn't abort transactions)
            try:
                labels = db.execute(text("""
                    SELECT e.enumlabel
                    FROM pg_type t
                    JOIN pg_enum e ON t.oid = e.enumtypid
                    WHERE t.typname = 'burstmethod'
                    ORDER BY e.enumsortorder
                """)).scalars().all()
                logger.info(f"🔤 burstmethod labels present: {labels}")
            except Exception as e:
                db.rollback()
                logger.warning(f"⚠️ Could not introspect enum: {e}")

            # (Optional) clear any error state before continuing
            try:
                db.rollback()
            except Exception:
                pass

            inspector = inspect(engine)
            existing_tables = inspector.get_table_names()

            if "ngrams" not in existing_tables:
                n_ngrams = 0
            else:
                try:
                    n_ngrams = db.execute(text("SELECT COUNT(*) FROM ngrams")).scalar() or 0
                except Exception as e:
                    logger.warning(f"⚠️ Could not count ngrams: {e}")
                    db.rollback()
                    n_ngrams = 0

            status = {
                "ngrams_count": n_ngrams,
                "needs_preprocessing": n_ngrams == 0,
                "preprocessing_completed": False,
                "preprocessing_time": 0.0
            }

            if n_ngrams == 0:
                logger.info("📦 Database empty, starting preprocessing...")
                status.update(self._run_preprocessing(db))
            else:
                logger.info(f"✅ Database already populated with {n_ngrams:,} ngrams!")
                status["preprocessing_completed"] = True

            return status

        except Exception as e:
            logger.error(f"❌ Database initialization failed: {e}")
            if "burstmethod" in str(e).lower() or "enum" in str(e).lower():
                logger.error("🔧 This appears to be an enum type issue.")
                logger.error("📋 To fix this, run:")
                logger.error("   DROP TABLE IF EXISTS burst_points CASCADE;")
                logger.error("   DROP TABLE IF EXISTS burst_detections CASCADE;")
                logger.error("   DROP TYPE IF EXISTS burstmethod CASCADE;")
                logger.error("💡 Then restart the application")
            return {
                "ngrams_count": 0,
                "needs_preprocessing": True,
                "preprocessing_completed": False,
                "preprocessing_time": 0.0,
                "error": str(e)
            }

    def _run_preprocessing(self, db: Session) -> dict:
        """Run the preprocessing pipeline."""
        try:
            start_time = datetime.now()

            # Initialize components
            loader = NgramFileLoader()
            resolver = SubfieldHierarchyResolver()
            preprocessor = UltraFastPreprocessor(loader, resolver, engine)

            logger.info("⏱️ Starting preprocessing...")
            preprocessor.run(db)

            end_time = datetime.now()
            preprocessing_time = (end_time - start_time).total_seconds()

            # Re-count ngrams after preprocessing
            n_ngrams = db.execute(text("SELECT COUNT(*) FROM ngrams")).scalar()

            logger.info(f"✅ Preprocessing completed in {preprocessing_time:.2f} seconds.")
            logger.info(f"✅ Database populated with {n_ngrams:,} ngrams!")

            return {
                "preprocessing_completed": True,
                "preprocessing_time": preprocessing_time,
                "ngrams_count": n_ngrams
            }

        except Exception as e:
            logger.error(f"❌ Preprocessing failed: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return {
                "preprocessing_completed": False,
                "preprocessing_time": 0.0,
                "error": str(e)
            }

    def initialize_burst_detection(self, db: Session, run_both: bool = True) -> dict:
        """Initialize burst detection analysis."""
        logger.info("🔍 Checking burst detection status...")

        status = self.burst_manager.get_detection_status(db)

        if not self.burst_manager.has_cached_data():
            logger.warning("⚠️ No cached data found!")
            logger.info("💡 Cached data is created during preprocessing")
            logger.info("🔄 Restart the app after preprocessing completes to run burst detection")
            return {
                "burst_detection_completed": False,
                "error": "No cached data available",
                **status
            }

        start_time = datetime.now()

        try:
            if run_both:
                results = self.burst_manager.run_both_methods(db)
                success = all(results.values())
            else:
                # Default to Kleinberg only for backward compatibility
                results = {"kleinberg": self.burst_manager.run_kleinberg_detection(db)}
                success = results["kleinberg"]

            end_time = datetime.now()
            detection_time = (end_time - start_time).total_seconds()

            # Get final status
            final_status = self.burst_manager.get_detection_status(db)

            return {
                "burst_detection_completed": success,
                "detection_time": detection_time,
                "methods_run": results,
                **final_status
            }

        except Exception as e:
            logger.error(f"❌ Burst detection initialization failed: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")

            return {
                "burst_detection_completed": False,
                "detection_time": 0.0,
                "error": str(e),
                **status
            }

    # ----------------- Voting data loaders -----------------

    def load_slider_vote_data(self, app_state) -> dict:
        """Load slider vote data configuration (slider_vote_data.json)."""
        try:
            # Prevent double load across hot-reloads/workers
            if getattr(app_state, "_slider_vote_loaded", False):
                logger.info("↪️ slider_vote_data already loaded; skipping.")
                return {
                    "slider_vote_data_loaded": True,
                    "total_pairs": len(getattr(app_state, "slider_vote_data", []) or []),
                    "skipped": True,
                }

            app_dir = os.path.dirname(os.path.abspath(__file__))
            backend_dir = os.path.dirname(app_dir)
            slider_pairs_path = os.path.join(backend_dir, "slider_vote_data.json")

            if os.path.exists(slider_pairs_path):
                with open(slider_pairs_path, "r", encoding="utf-8") as f:
                    slider_data = json.load(f)

                _log_slider_stats("raw", slider_data)
                slider_data = _dedup_by_id(slider_data)

                app_state.slider_vote_data = slider_data
                app_state._slider_vote_loaded = True

                _log_slider_stats("final", app_state.slider_vote_data)
                logger.info(f"🗳️ Loaded {len(app_state.slider_vote_data)} ngrams for slider voting.")

                return {
                    "slider_vote_data_loaded": True,
                    "total_pairs": len(app_state.slider_vote_data),
                }
            else:
                app_state.slider_vote_data = []
                app_state._slider_vote_loaded = True
                logger.warning("⚠️ slider_vote_data.json not found. Slider voting will be disabled.")
                return {
                    "slider_vote_data_loaded": False,
                    "total_pairs": 0
                }

        except Exception as e:
            app_state.slider_vote_data = []
            app_state._slider_vote_loaded = True
            logger.error(f"❌ Failed to load slider_vote_data.json: {e}")
            return {
                "slider_vote_data_loaded": False,
                "error": str(e),
                "total_pairs": 0
            }

    def load_binary_vote_data(self, app_state) -> dict:
        """Load binary vote pairs configuration (binary_vote_data.json)."""
        try:
            app_dir = os.path.dirname(os.path.abspath(__file__))
            backend_dir = os.path.dirname(app_dir)
            binary_pairs_path = os.path.join(backend_dir, "binary_vote_data.json")

            if os.path.exists(binary_pairs_path):
                with open(binary_pairs_path, "r", encoding="utf-8") as f:
                    binary_data = json.load(f)

                app_state.binary_vote_data = binary_data
                logger.info(f"🗳️ Loaded {len(binary_data)} binary vote pairs.")
                return {
                    "binary_vote_data_loaded": True,
                    "total_pairs": len(binary_data),
                }
            else:
                app_state.binary_vote_data = []
                logger.warning("⚠️ binary_vote_data.json not found. Binary voting will be disabled.")
                return {
                    "binary_vote_data_loaded": False,
                    "total_pairs": 0
                }

        except Exception as e:
            app_state.binary_vote_data = []
            logger.error(f"❌ Failed to load binary_vote_data.json: {e}")
            return {
                "binary_vote_data_loaded": False,
                "error": str(e),
                "total_pairs": 0
            }

    def get_initialization_summary(self, db: Session) -> dict:
        """Get summary of current initialization status."""
        try:
            # Database status
            n_ngrams = db.execute(text("SELECT COUNT(*) FROM ngrams")).scalar() or 0

            # Burst detection status
            burst_status = self.burst_manager.get_detection_status(db)

            # Cache status
            cache_info = self.burst_manager.get_cache_info()

            return {
                "database": {
                    "ngrams": n_ngrams,
                    "initialized": n_ngrams > 0
                },
                "burst_detection": burst_status,
                "cache": cache_info,
                "burst_methods_available": ["kleinberg", "macd"]
            }

        except Exception as e:
            logger.error(f"Failed to get initialization summary: {e}")
            return {"error": str(e)}
