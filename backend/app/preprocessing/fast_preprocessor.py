# File: app/preprocessing/fast_preprocessor.py

import pandas as pd
import gc
import re
import json
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.models import Domain, Field, Subfield, Ngram, TimeSeries
from app.preprocessing.token_filter import SafeTokenFilter
from settings import BASE_DIR
import logging
import io
import csv

logger = logging.getLogger("uvicorn")

class UltraFastPreprocessor:
    """
    Ultra-efficient preprocessor with safe n-gram-level token filtering.
    
    Methodology for n-gram-level filtering:
    1. Check if n-gram contains ANY non-stopword tokens
    2. If yes: Keep entire n-gram unchanged
    3. If no: Drop entire n-gram (consists only of stopwords)
    """

    def __init__(self, loader, resolver, engine):
        self.loader = loader
        self.resolver = resolver
        self.engine = engine
        self.cache_dir = BASE_DIR / "cache"
        self.cache_dir.mkdir(exist_ok=True)
        
        # Initialize safe token filter
        self.token_filter = SafeTokenFilter()
        filter_stats = self.token_filter.get_filter_stats()
        logger.info(f"🔧 Safe n-gram-level token filter initialized:")
        logger.info(f"   - HTML/XML artifacts: {filter_stats['html_xml_artifacts']}")
        logger.info(f"   - MathML tokens: {filter_stats['mathml_tokens']}")
        logger.info(f"   - URL fragments: {filter_stats['url_fragments']}")
        logger.info(f"   - Safe foreign stopwords: {filter_stats['safe_foreign_stopwords']}")
        logger.info(f"   - Total stopword tokens: {filter_stats['total_explicit_tokens']}")

        # Tunables
        self.hierarchy_chunk_size = 1000
        self.ngram_chunk_size = 100_000
        self.timeseries_chunk_size = 100_000

        logger.info("🚀 UltraFast: N-gram-level filtering (keep unchanged or drop entirely)")

    def run(self, db: Session):
        """Ultra-efficient preprocessing pipeline with n-gram-level filtering."""
        start_time = datetime.now()

        # Load and process data
        logger.info("🚀 Stage 1: Loading data...")
        df = self.loader.load_files()
        logger.info(f"📊 Loaded {len(df):,} rows with {len(df.columns)} columns")

        logger.info("🚀 Stage 2: Resolving hierarchy...")
        df = self._resolve_hierarchy(df)

        logger.info("🚀 Stage 3: N-gram-level filtering...")
        df, filter_mapping = self._clean_and_filter_data_ngram_level(df)

        # 🚫 Drop all 2025 time columns before caching & SQL load
        logger.info("🗓️  Pruning time columns for year 2025, because of inconsistent data…")
        df = self._drop_year_2025(df)

        # 🧽 Remove n-grams with zero counts across all remaining periods
        df, zero_drop_count = self._drop_all_zero_timeseries(df)
        logger.info(f"🧮 Removed {zero_drop_count:,} all-zero time-series rows")


        logger.info("🚀 Stage 4: Caching processed data and filter mapping...")
        self._cache_data_and_mapping(df, filter_mapping)

        logger.info("🚀 Stage 5: Ultra-efficient database insertion...")
        self._insert_data_ultra_efficient(df, db)

        total_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"✅ Preprocessing completed in {total_time:.1f} seconds!")

    def _resolve_hierarchy(self, df: pd.DataFrame) -> pd.DataFrame:
        """Resolve hierarchy efficiently."""
        subfield_urls = df["Subfield"].unique()
        logger.info(f"🔗 Resolving {len(subfield_urls)} unique subfield URLs...")

        hierarchy = self.resolver.resolve_subfields(subfield_urls)
        hierarchy_df = pd.DataFrame.from_dict(hierarchy, orient='index')
        hierarchy_df = hierarchy_df.reset_index().rename(columns={'index': 'Subfield'})

        # Ensure all columns exist
        for col in ['subfield', 'field', 'domain']:
            if col not in hierarchy_df.columns:
                hierarchy_df[col] = ""

        df = df.merge(hierarchy_df[['Subfield', 'subfield', 'field', 'domain']],
                      on="Subfield", how="left")
        
        # Drop the original Subfield URL column - we only need the resolved names
        df = df.drop(columns=['Subfield'])
        
        logger.info(f"🔁 Hierarchy resolved: {len(hierarchy)} subfields")
        return df

    def _clean_and_filter_data_ngram_level(self, df: pd.DataFrame) -> tuple:
        """
        Clean data with n-gram-level filtering.
        
        Returns:
            (cleaned_df, filter_mapping): Cleaned dataframe and mapping of changes
        """
        initial_len = len(df)

        # Remove rows with missing n-grams
        before_nan_removal = len(df)
        df = df[df["n-gram"].notna()].copy()
        after_nan_removal = len(df)
        nan_removed = before_nan_removal - after_nan_removal
        
        df["n-gram"] = df["n-gram"].astype(str).str.lower()

        # Fill missing hierarchy values
        df["domain"] = df["domain"].fillna("")
        df["field"] = df["field"].fillna("")
        df["subfield"] = df["subfield"].fillna("")

        # Apply n-gram-level token filtering
        logger.info("🧹 Applying n-gram-level safe token filtering...")
        
        kept_ngrams = []
        completely_filtered = 0
        kept_unchanged = 0
        
        # Track filtering results for the mapping
        filter_mapping = {
            "completely_filtered": [],
            "stats": {
                "total_processed": 0,
                "nan_removed": nan_removed,
                "completely_filtered": 0,
                "kept_unchanged": 0
            }
        }
        
        for original_ngram in df["n-gram"]:
            filtered_result, was_dropped = self.token_filter.filter_ngram_with_tracking(original_ngram)
            
            filter_mapping["stats"]["total_processed"] += 1
            
            if was_dropped:  # N-gram consists entirely of stopwords
                completely_filtered += 1
                filter_mapping["completely_filtered"].append(original_ngram)
                filter_mapping["stats"]["completely_filtered"] += 1
                kept_ngrams.append("")  # Mark for removal
            else:  # N-gram contains at least one non-stopword - keep unchanged
                kept_unchanged += 1
                filter_mapping["stats"]["kept_unchanged"] += 1
                kept_ngrams.append(filtered_result)  # Should be same as original
        
        # Update the n-gram column
        df["n-gram"] = kept_ngrams
        
        # Remove rows where n-grams were completely filtered out
        df_final = df[df["n-gram"].str.len() > 0].copy()
        
        # Calculate n_words
        df_final["n_words"] = df_final["n-gram"].str.count(" ") + 1
        
        # Calculate final statistics
        total_removed = initial_len - len(df_final)
        
        logger.info(f"🧹 Cleaning and filtering results:")
        if nan_removed > 0:
            logger.info(f"   - {nan_removed:,} rows with missing n-grams removed")
        logger.info(f"   - {completely_filtered:,} n-grams completely dropped (pure stopwords)")
        logger.info(f"   - {kept_unchanged:,} n-grams kept unchanged")
        logger.info(f"⚠️ Total reduction: {total_removed:,} rows ({total_removed/initial_len*100:.1f}%)")
        logger.info(f"✨ Final cleaned data: {len(df_final):,} rows")
        
        # Update final stats
        filter_mapping["stats"]["final_rows"] = len(df_final)
        filter_mapping["stats"]["total_removed"] = total_removed
        filter_mapping["stats"]["removal_percentage"] = total_removed/initial_len*100
        
        return df_final, filter_mapping
    
    def _drop_year_2025(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove any time columns that belong to year 2025.
        Works with Timestamp columns and string-like labels such as '2025', '2025-05', '2025-05-01'.
        """
        time_cols = self._detect_time_columns(df)
        cols_to_drop = []

        for c in time_cols:
            # Convert the label to an ISO date string (e.g., '2025-01-01') then parse it
            ds = self._col_to_date(c)
            try:
                dt = pd.to_datetime(ds, errors="raise")
                if dt.year == 2025:
                    cols_to_drop.append(c)
            except Exception:
                # If it can't be parsed, don't treat it as a time column here
                pass

        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)
            logger.info(f"🧽 Removed {len(cols_to_drop)} time columns for year 2025")

        return df

    def _drop_all_zero_timeseries(self, df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        """
        Remove n-grams whose entire time series is zero (after pruning 2025).
        Returns (filtered_df, removed_count).
        """
        time_cols = self._detect_time_columns(df)
        if not time_cols:
            logger.warning("⚠️ No time columns detected; skipping all-zero time-series drop.")
            return df, 0

        mask_any_pos = (df[time_cols].fillna(0) > 0).any(axis=1)
        removed_count = int((~mask_any_pos).sum())
        filtered = df[mask_any_pos].copy()

        logger.info(f"🧽 Dropped {removed_count:,} n-grams with zero counts across all kept periods")
        return filtered, removed_count


    def _cache_data_and_mapping(self, df: pd.DataFrame, filter_mapping: dict):
        """Cache processed data and filter mapping."""
        # Cache processed data
        cache_file = self.cache_dir / "processed_ngram_data.parquet"
        df.to_parquet(cache_file, compression='gzip', index=False)

        # Cache filter mapping
        mapping_file = self.cache_dir / "token_filter_mapping.json"
        with open(mapping_file, 'w', encoding='utf-8') as f:
            json.dump(filter_mapping, f, indent=2, ensure_ascii=False)

        time_cols = self._detect_time_columns(df)
        logger.info(f"💾 Cached: {len(df):,} ngrams, with {len(time_cols)} time periods")
        logger.info(f"💾 Saved filter mapping to: {mapping_file}")
        logger.info(f"   - {filter_mapping['stats']['completely_filtered']:,} n-grams completely filtered")
        logger.info(f"   - {filter_mapping['stats']['kept_unchanged']:,} n-grams kept unchanged")

    def _detect_time_columns(self, df: pd.DataFrame):
        """Detect time columns efficiently."""
        timestamp_cols = [c for c in df.columns if isinstance(c, pd.Timestamp)]
        remaining_cols = [c for c in df.columns if c not in timestamp_cols]
        pattern = r"^\d{4}(-\d{2}(-\d{2})?)?$"
        date_pattern_cols = [c for c in remaining_cols if re.match(pattern, str(c))]
        return timestamp_cols + date_pattern_cols

    def _col_to_date(self, label) -> str:
        """Convert column label to date string."""
        if isinstance(label, pd.Timestamp):
            return label.strftime("%Y-%m-%d")
        s = str(label)
        if re.match(r"^\d{4}$", s):
            return f"{s}-01-01"
        try:
            return pd.to_datetime(s).strftime("%Y-%m-%d")
        except Exception:
            return s

    def _insert_data_ultra_efficient(self, df: pd.DataFrame, db: Session):
        """Ultra-efficient insertion using the fastest PostgreSQL methods."""
        logger.info("🚀 Step 1: Inserting hierarchy (UPSERT method)...")
        self._insert_hierarchy_upsert(df, db)

        logger.info("🚀 Step 2: Building ngram ID mapping...")
        ngram_id_map = self._build_and_insert_ngrams_efficiently(df, db)

        logger.info("🚀 Step 3: Ultra-fast time series insertion (COPY method, fast path)...")
        self._insert_timeseries_fastpath(df, db, ngram_id_map)

    def _insert_hierarchy_upsert(self, df: pd.DataFrame, db: Session):
        """Insert hierarchy using PostgreSQL UPSERT (ON CONFLICT DO NOTHING)."""
        try:
            # Get unique values
            domains = df["domain"].dropna().unique()
            fields_data = df[["field", "domain"]].dropna().drop_duplicates()
            subfields_data = df[["subfield", "field", "domain"]].dropna().drop_duplicates()

            # Insert domains
            if len(domains) > 0:
                domain_values = [{"name": d} for d in domains]
                for domain_batch in [domain_values[i:i+self.hierarchy_chunk_size]
                                     for i in range(0, len(domain_values), self.hierarchy_chunk_size)]:
                    db.execute(text("""
                        INSERT INTO domains (name)
                        VALUES (:name)
                        ON CONFLICT (name) DO NOTHING
                    """), domain_batch)

            # Insert fields
            field_values = [{"field_name": row["field"], "domain_name": row["domain"]}
                            for _, row in fields_data.iterrows()]
            for field_batch in [field_values[i:i+self.hierarchy_chunk_size]
                                for i in range(0, len(field_values), self.hierarchy_chunk_size)]:
                db.execute(text("""
                    INSERT INTO fields (name, domain_id)
                    SELECT :field_name, d.id
                    FROM domains d
                    WHERE d.name = :domain_name
                    ON CONFLICT (name, domain_id) DO NOTHING
                """), field_batch)

            # Insert subfields
            subfield_values = [{"subfield_name": row["subfield"],
                                "field_name": row["field"],
                                "domain_name": row["domain"]}
                               for _, row in subfields_data.iterrows()]
            for subfield_batch in [subfield_values[i:i+self.hierarchy_chunk_size]
                                   for i in range(0, len(subfield_values), self.hierarchy_chunk_size)]:
                db.execute(text("""
                    INSERT INTO subfields (name, field_id)
                    SELECT :subfield_name, f.id
                    FROM domains d
                    JOIN fields f ON f.name = :field_name AND f.domain_id = d.id
                    WHERE d.name = :domain_name
                    ON CONFLICT (field_id, name) DO NOTHING
                """), subfield_batch)

            db.commit()
            logger.info("✅ Hierarchy inserted with UPSERT")

        except Exception as e:
            db.rollback()
            logger.error(f"❌ Hierarchy insertion failed: {e}")
            raise

    def _build_and_insert_ngrams_efficiently(self, df: pd.DataFrame, db: Session) -> dict:
        """Build ngrams and return ID mapping efficiently (batch INSERT + one SQL fetch)."""

        ngram_cols = ["n-gram", "n_words", "domain", "field", "subfield", "DF(n-gram)", "DF(n-gram, subfield)"]
        ngrams_df = df[ngram_cols].drop_duplicates(subset=["n-gram", "domain", "field", "subfield"])

        logger.info(f"🔄 Processing {len(ngrams_df):,} unique ngrams...")

        try:
            total_chunks = (len(ngrams_df) + self.ngram_chunk_size - 1) // self.ngram_chunk_size

            for i in range(0, len(ngrams_df), self.ngram_chunk_size):
                chunk = ngrams_df.iloc[i:i + self.ngram_chunk_size]
                chunk_num = i // self.ngram_chunk_size + 1

                logger.info(f"🔄 Inserting ngram chunk {chunk_num}/{total_chunks} ({len(chunk):,} rows)")

                ngram_batch = []
                for _, row in chunk.iterrows():
                    ngram_batch.append({
                        'text': row["n-gram"],
                        'n_words': int(row["n_words"]),
                        'df_ngram': float(row["DF(n-gram)"]),
                        'df_ngram_subfield': float(row["DF(n-gram, subfield)"]),
                        'domain': row["domain"],
                        'field': row["field"],
                        'subfield': row["subfield"]
                    })

                db.execute(text("""
                    INSERT INTO ngrams (text, n_words, df_ngram, df_ngram_subfield, subfield_id)
                    SELECT :text, :n_words, :df_ngram, :df_ngram_subfield, s.id
                    FROM domains d
                    JOIN fields f ON f.name = :field AND f.domain_id = d.id
                    JOIN subfields s ON s.name = :subfield AND s.field_id = f.id
                    WHERE d.name = :domain
                    ON CONFLICT (text, subfield_id) DO NOTHING
                """), ngram_batch)

                db.commit()
                del chunk, ngram_batch
                if chunk_num % 5 == 0:
                    gc.collect()

            # Build ID mapping with a single query
            logger.info("🔗 Building ngram ID mapping...")
            query_result = db.execute(text("""
                SELECT n.id, n.text, d.name as domain, f.name as field, s.name as subfield
                FROM ngrams n
                JOIN subfields s ON n.subfield_id = s.id
                JOIN fields f ON s.field_id = f.id
                JOIN domains d ON f.domain_id = d.id
            """)).fetchall()

            ngram_id_map = {}
            for row in query_result:
                key = (row.text, row.domain, row.field, row.subfield)
                ngram_id_map[key] = row.id

            logger.info(f"✅ Built ID mapping for {len(ngram_id_map):,} ngrams")
            return ngram_id_map

        except Exception as e:
            db.rollback()
            logger.error(f"❌ Ngram insertion failed: {e}")
            raise

    def _insert_timeseries_fastpath(self, df: pd.DataFrame, db: Session, ngram_id_map: dict):
        """
        Ultra-fast time series insertion assuming the target is empty:
        - Drop ORM-created empty table to avoid name conflicts
        - Create UNLOGGED build table without constraints/indexes
        - COPY CSV stream (no melt)
        - Add id/constraints/indexes with final names
        - Rename build -> time_series
        """
        logger.info("⚡ Fast path: empty time_series → bulk load without constraints/indexes")

        time_cols = self._detect_time_columns(df)
        date_strings = [self._col_to_date(c) for c in time_cols]

        try:
            # 0) Remove any previous leftovers to ensure idempotency
            db.execute(text("DROP TABLE IF EXISTS time_series_build"))
            db.commit()

            # 1) Drop the empty ORM-created target to free constraint/index names
            #    (safe because you guaranteed the table is empty on first run)
            db.execute(text("DROP TABLE IF EXISTS time_series"))
            db.commit()

            # 2) Build table: UNLOGGED, no constraints; disable autovacuum during load
            db.execute(text("""
                CREATE UNLOGGED TABLE time_series_build (
                    ngram_id INTEGER NOT NULL,
                    date     DATE    NOT NULL,
                    count    DOUBLE PRECISION NOT NULL
                );
                ALTER TABLE time_series_build SET (autovacuum_enabled = off);
            """))
            db.commit()

            # 3) Stream -> CSV -> COPY
            total_chunks = (len(df) + self.timeseries_chunk_size - 1) // self.timeseries_chunk_size
            logger.info(f"⚡ Using COPY into time_series_build - {len(time_cols)} time periods")

            processed_rows = 0
            raw_conn = db.connection().connection

            for i in range(0, len(df), self.timeseries_chunk_size):
                chunk = df.iloc[i:i + self.timeseries_chunk_size]
                chunk_num = (i // self.timeseries_chunk_size) + 1
                logger.info(f"🔄 COPY chunk {chunk_num}/{total_chunks} ({len(chunk):,} ngrams)")

                csv_buffer = io.StringIO()
                writer = csv.writer(csv_buffer, lineterminator="\n")

                time_values = chunk[time_cols].to_numpy()
                texts = chunk["n-gram"].values
                domains = chunk["domain"].values
                fields = chunk["field"].values
                subfields = chunk["subfield"].values

                for r in range(len(chunk)):
                    nid = ngram_id_map.get((texts[r], domains[r], fields[r], subfields[r]))
                    if nid is None:
                        continue
                    row_vals = time_values[r]
                    for j, val in enumerate(row_vals):
                        if pd.notna(val):
                            writer.writerow([int(nid), date_strings[j], float(val)])
                            processed_rows += 1

                if csv_buffer.tell() > 0:
                    csv_buffer.seek(0)
                    cur = raw_conn.cursor()
                    try:
                        cur.copy_expert(
                            "COPY time_series_build (ngram_id, date, count) FROM STDIN WITH (FORMAT CSV)",
                            csv_buffer
                        )
                        raw_conn.commit()
                    except Exception as e:
                        raw_conn.rollback()
                        logger.error(f"❌ COPY failed for chunk {chunk_num}: {e}")
                        raise
                    finally:
                        cur.close()

                del chunk, csv_buffer, time_values, texts, domains, fields, subfields
                if (chunk_num % 3) == 0:
                    gc.collect()

            logger.info(f"✅ COPY finished into build table, ~{processed_rows:,} rows")

            # 4) Finalize schema (add id, constraints, indexes with final names)
            logger.info("🧱 Finalizing schema on build table (id, PK, unique, indexes)…")
            db.execute(text("""
                ALTER TABLE time_series_build
                ADD COLUMN id BIGSERIAL;

                UPDATE time_series_build SET id = DEFAULT WHERE id IS NULL;

                ALTER TABLE time_series_build
                ADD CONSTRAINT time_series_pk PRIMARY KEY (id);

                -- Use the final, ORM-expected names now that the old table is gone
                ALTER TABLE time_series_build
                ADD CONSTRAINT uq_ngram_date UNIQUE (ngram_id, date);

                CREATE INDEX idx_timeseries_ngram_date ON time_series_build (ngram_id, date);
                CREATE INDEX idx_timeseries_date       ON time_series_build (date);

                ALTER TABLE time_series_build SET LOGGED;
                ALTER TABLE time_series_build SET (autovacuum_enabled = on);
            """))
            db.commit()

            # 5) Publish (simple rename since the old table was already dropped)
            logger.info("🔁 Publishing build table as time_series…")
            db.execute(text("ALTER TABLE time_series_build RENAME TO time_series;"))
            db.commit()

            db.execute(text("ANALYZE time_series;"))
            db.commit()

            logger.info("✅ Time series published successfully")

        except Exception as e:
            db.rollback()
            logger.error(f"❌ Fast-path time series load failed: {e}")
            raise