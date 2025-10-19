# app/crud/burst_crud.py
"""
CRUD operations for burst detection data.
Returns raw dictionaries - API layer handles Pydantic conversion.
"""

from typing import Optional, Tuple, List, Dict, Any
from datetime import date, timedelta

from sqlalchemy.orm import Session
from sqlalchemy import func, text

from app.models.burst_point import BurstPoint
from app.models.burst import BurstDetection
from app.schemas.bursts import BurstMethod


def get_interval_score(
    db: Session,
    ngram_id: int,
    method: BurstMethod,
    start: Optional[date],
    end: Optional[date],
) -> float:
    """
    Sum of contributions strictly inside [start, end).
    """
    q = db.query(func.coalesce(func.sum(BurstPoint.contribution), 0.0)).filter(
        BurstPoint.ngram_id == ngram_id,
        BurstPoint.method == method,
    )
    if start:
        q = q.filter(BurstPoint.date >= start)
    if end:
        q = q.filter(BurstPoint.date < (end + timedelta(days=1)))
    return float(q.scalar() or 0.0)


def get_global_score(
    db: Session,
    ngram_id: int,
    method: BurstMethod
) -> Optional[float]:
    """Get pre-computed global score from burst_detections table."""
    det = db.query(BurstDetection.global_score).filter(
        BurstDetection.ngram_id == ngram_id,
        BurstDetection.method == method
    ).first()
    return float(det[0]) if det else None


def get_points_in_range(
    db: Session,
    ngram_id: int,
    method: BurstMethod,
    start: Optional[date],
    end: Optional[date],
    limit: int = 20000
) -> List[tuple]:
    """
    Get burst points with FULL MACD metrics or Kleinberg metrics.
    Returns list of tuples for efficient API layer processing.
    """
    q = db.query(
        BurstPoint.date,
        BurstPoint.period_index,
        BurstPoint.contribution,
        BurstPoint.raw_value,
        BurstPoint.baseline_value,
        # ✅ NEW: Complete MACD metrics
        BurstPoint.macd_short_ema,
        BurstPoint.macd_long_ema,
        BurstPoint.macd_line,
        BurstPoint.macd_signal,
        BurstPoint.macd_histogram,
        # Kleinberg metrics
        BurstPoint.kleinberg_state,
        BurstPoint.state_probability,
        BurstPoint.weight_contribution,
    ).filter(
        BurstPoint.ngram_id == ngram_id,
        BurstPoint.method == method,
    )
    if start:
        q = q.filter(BurstPoint.date >= start)
    if end:
        q = q.filter(BurstPoint.date < (end + timedelta(days=1)))

    return q.order_by(BurstPoint.date.asc()).limit(limit).all()


def get_burst_leaderboard(
    db: Session,
    method: BurstMethod,
    *,
    start: Optional[date] = None,
    end: Optional[date] = None,
    n_words: Optional[int] = None,
    subfield_id: Optional[int] = None,
    field_id: Optional[int] = None,
    domain_id: Optional[int] = None,
    ngram_text: Optional[str] = None,
    page: int = 1,
    page_size: int = 100,
    sort_order: str = "desc",
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Get burst leaderboard with hierarchy filters applied BEFORE normalization.
    
    Filter order:
    1. Method filter
    2. Hierarchy filters (domain/field/subfield) - AFFECTS NORMALIZATION
    3. Time range filter (if interval mode)
    4. N-words filter (if provided) - AFFECTS NORMALIZATION
    5. Aggregate scores
    6. Normalize within filtered scope (method + hierarchy + time + n_words)
    7. Apply text search filter (after normalization)
    8. Paginate
    
    Returns:
        Tuple of (list of result dicts, total count)
    """
    is_interval = (start is not None) and (end is not None)
    sort_dir = "DESC" if str(sort_order).lower() != "asc" else "ASC"

    method_str = (
        method if isinstance(method, str)
        else getattr(method, "value", str(method))
    )

    if is_interval:
        # -------- Interval mode: Single-pass with temp table --------
        end_plus_one = end + timedelta(days=1)

        temp_table_query = """
        CREATE TEMP TABLE IF NOT EXISTS _interval_scores (
            ngram_id INTEGER,
            text TEXT,
            n_words INTEGER,
            domain TEXT,
            domain_id INTEGER,
            field TEXT,
            field_id INTEGER,
            subfield TEXT,
            subfield_id INTEGER,
            score DOUBLE PRECISION
        ) ON COMMIT DROP;

        TRUNCATE TABLE _interval_scores;

        -- Apply ALL filters before aggregation (including hierarchy)
        INSERT INTO _interval_scores
        SELECT 
            n.id AS ngram_id,
            n.text,
            n.n_words,
            d.name AS domain,  d.id AS domain_id,
            f.name AS field,   f.id AS field_id,
            s.name AS subfield, s.id AS subfield_id,
            SUM(bp.contribution) AS score
        FROM burst_points bp
        JOIN ngrams n    ON n.id = bp.ngram_id
        JOIN subfields s ON n.subfield_id = s.id
        JOIN fields f    ON s.field_id = f.id
        JOIN domains d   ON f.domain_id = d.id
        WHERE bp.method = :method
          AND bp.date  >= :start
          AND bp.date  <  :end_plus_one
          AND bp.contribution > 0
          AND (:n_words IS NULL OR n.n_words = :n_words)
          AND (:domain_id IS NULL OR d.id = :domain_id)
          AND (:field_id IS NULL OR f.id = :field_id)
          AND (:subfield_id IS NULL OR s.id = :subfield_id)
        GROUP BY n.id, n.text, n.n_words, d.name, d.id, f.name, f.id, s.name, s.id
        HAVING SUM(bp.contribution) > 0;
        """

        minmax_query = "SELECT MIN(score), MAX(score) FROM _interval_scores;"

        # Normalize, then apply text search
        sql_query = f"""
        WITH normalized AS (
            SELECT *,
                CASE 
                    WHEN :max_score = :min_score THEN 50.0
                    ELSE ((score - :min_score) / NULLIF(:max_score - :min_score, 0)) * 100.0
                END AS normalized_score
            FROM _interval_scores
        )
        SELECT * FROM normalized
        WHERE (:ngram_text IS NULL OR LOWER(text) = LOWER(:ngram_text))
        ORDER BY normalized_score {sort_dir}, score {sort_dir}, ngram_id ASC
        LIMIT :page_size OFFSET :offset;
        """

        count_query = """
        SELECT COUNT(*) FROM _interval_scores
        WHERE (:ngram_text IS NULL OR LOWER(text) = LOWER(:ngram_text));
        """

        # ✅ FIX: Always include ngram_text in params
        params = {
            "method": method_str,
            "start": start,
            "end_plus_one": end_plus_one,
            "n_words": n_words,
            "domain_id": domain_id,
            "field_id": field_id,
            "subfield_id": subfield_id,
            "ngram_text": ngram_text,  # ✅ CRITICAL: Always include even if None
            "page_size": page_size,
            "offset": (page - 1) * page_size,
        }

        try:
            db.execute(text(temp_table_query), params)

            minmax_result = db.execute(text(minmax_query)).first()
            if not minmax_result or minmax_result[0] is None:
                return [], 0

            params["min_score"] = float(minmax_result[0])
            params["max_score"] = float(minmax_result[1])

            total = int(db.execute(text(count_query), params).scalar() or 0)
            if total == 0:
                return [], 0

            results = db.execute(text(sql_query), params).mappings().all()

        except Exception as e:
            print(f"❌ Database query error in get_burst_leaderboard (interval): {e}")
            import traceback
            print(traceback.format_exc())
            raise

    else:
        # -------- Global mode: Window functions with hierarchy filters --------
        partition_cols = ["method"]
        if n_words:
            partition_cols.append("n_words")
        
        # Add hierarchy to partition if any filter is applied
        if domain_id or field_id or subfield_id:
            if subfield_id:
                partition_cols.append("subfield_id")
            elif field_id:
                partition_cols.append("field_id")
            elif domain_id:
                partition_cols.append("domain_id")
        
        partition_clause = "PARTITION BY " + ", ".join(partition_cols)

        sql_query = f"""
        WITH scored AS (
            SELECT 
                n.id AS ngram_id,
                n.text,
                n.n_words,
                d.name AS domain, d.id AS domain_id,
                f.name AS field, f.id AS field_id,
                s.name AS subfield, s.id AS subfield_id,
                bd.method,
                bd.global_score AS score,
                bd.num_bursts,
                bd.rank
            FROM ngrams n
            JOIN subfields s ON n.subfield_id = s.id
            JOIN fields f ON s.field_id = f.id
            JOIN domains d ON f.domain_id = d.id
            JOIN burst_detections bd ON bd.ngram_id = n.id AND bd.method = :method
            WHERE 1=1
            {"AND n.n_words = :n_words" if n_words else ""}
            {"AND d.id = :domain_id" if domain_id else ""}
            {"AND f.id = :field_id" if field_id else ""}
            {"AND s.id = :subfield_id" if subfield_id else ""}
        ),
        normalized AS (
            SELECT *,
                CASE 
                    WHEN MAX(score) OVER ({partition_clause}) = MIN(score) OVER ({partition_clause})
                    THEN 50.0
                    ELSE ((score - MIN(score) OVER ({partition_clause})) /
                          NULLIF(MAX(score) OVER ({partition_clause}) - MIN(score) OVER ({partition_clause}), 0)) * 100.0
                END AS normalized_score
            FROM scored
        )
        SELECT * FROM normalized
        WHERE (:ngram_text IS NULL OR LOWER(text) = LOWER(:ngram_text))
        ORDER BY normalized_score {sort_dir}, score {sort_dir}, ngram_id ASC
        LIMIT :page_size OFFSET :offset
        """

        count_query = f"""
        SELECT COUNT(*)
        FROM ngrams n
        JOIN subfields s ON n.subfield_id = s.id
        JOIN fields f ON s.field_id = f.id
        JOIN domains d ON f.domain_id = d.id
        JOIN burst_detections bd ON bd.ngram_id = n.id AND bd.method = :method
        WHERE 1=1
        {"AND n.n_words = :n_words" if n_words else ""}
        {"AND d.id = :domain_id" if domain_id else ""}
        {"AND f.id = :field_id" if field_id else ""}
        {"AND s.id = :subfield_id" if subfield_id else ""}
        {"AND LOWER(n.text) = LOWER(:ngram_text)" if ngram_text else ""}
        """

        # ✅ FIX: Always include ngram_text in params
        params = {
            "method": method_str,
            "ngram_text": ngram_text,  # ✅ CRITICAL: Always include even if None
            "page_size": page_size,
            "offset": (page - 1) * page_size,
        }
        if n_words is not None:
            params["n_words"] = n_words
        if subfield_id is not None:
            params["subfield_id"] = subfield_id
        if field_id is not None:
            params["field_id"] = field_id
        if domain_id is not None:
            params["domain_id"] = domain_id

        try:
            total = int(db.execute(text(count_query), params).scalar() or 0)
            if total == 0:
                return [], 0

            results = db.execute(text(sql_query), params).mappings().all()

        except Exception as e:
            print(f"❌ Database query error in get_burst_leaderboard (global): {e}")
            import traceback
            print(traceback.format_exc())
            raise

    # Convert SQLAlchemy row mappings to plain dicts
    rows: List[Dict[str, Any]] = []
    for r in results:
        rows.append({
            "ngram_id": int(r["ngram_id"]),
            "text": str(r["text"]),
            "n_words": int(r["n_words"]),
            "domain": str(r.get("domain") or ""),
            "domain_id": int(r["domain_id"]),
            "field": str(r.get("field") or ""),
            "field_id": int(r["field_id"]),
            "subfield": str(r.get("subfield") or ""),
            "subfield_id": int(r["subfield_id"]),
            "method": str(r.get("method") or method_str),
            "score": float(r["score"]),
            "normalized_score": float(r["normalized_score"]),
            "num_bursts": r.get("num_bursts"),
            "rank": r.get("rank"),
        })

    return rows, total