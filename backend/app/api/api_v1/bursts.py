# app/api/bursts.py
"""
FastAPI routes for burst detection endpoints.
"""

from datetime import date
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.core.database import SessionLocal
from app.crud import burst_crud
from app.schemas.bursts import (
    BurstMethod, BurstScoreResponse, BurstPointsResponse, BurstPointDTO,
    BurstLeaderboardResponse, BurstLeaderboardRow
)
from app.schemas import PaginationMeta
from app.models.burst_point import BurstPoint

router = APIRouter()


def get_db():
    """Database session dependency."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _resolve_window(
    db: Session,
    start: date | None,
    end: date | None
) -> tuple[date | None, date | None]:
    """
    Resolve partial time windows to dataset boundaries.
    
    - If both None: return (None, None) for global mode
    - If one is None: fill with dataset min/max
    - Validate start <= end
    """
    if start is None and end is None:
        return None, None

    min_date, max_date = db.query(
        func.min(BurstPoint.date),
        func.max(BurstPoint.date)
    ).one()

    if min_date is None or max_date is None:
        return None, None

    if start is None:
        start = min_date
    if end is None:
        end = max_date

    if start > end:
        raise HTTPException(status_code=400, detail="start must be <= end")

    return start, end


@router.get("/bursts/score", response_model=BurstScoreResponse)
def get_burst_score(
    ngram_id: int = Query(..., gt=0, description="N-gram ID"),
    method: BurstMethod = Query("macd", description="Burst detection method"),
    start: date | None = Query(None, description="Start date (inclusive)"),
    end: date | None = Query(None, description="End date (inclusive)"),
    db: Session = Depends(get_db),
):
    """
    Get burst score for a specific n-gram in a time range.
    
    - If start/end provided: sum contributions in [start, end]
    - If both None: return pre-computed global score
    """
    start, end = _resolve_window(db, start, end)

    if start or end:
        score = burst_crud.get_interval_score(db, ngram_id, method, start, end)
    else:
        score = burst_crud.get_global_score(db, ngram_id, method)
        if score is None:
            score = 0.0

    return BurstScoreResponse(
        ngram_id=ngram_id,
        method=method,
        start=start,
        end=end,
        score=float(score)
    )


@router.get("/bursts/points", response_model=BurstPointsResponse)
def get_burst_points(
    ngram_id: int = Query(..., gt=0, description="N-gram ID"),
    method: BurstMethod = Query("macd", description="Burst detection method"),
    start: date | None = Query(None, description="Start date (inclusive)"),
    end: date | None = Query(None, description="End date (inclusive)"),
    limit: int = Query(20000, ge=1, le=200000, description="Max points to return"),
    db: Session = Depends(get_db),
):
    """
    Get time series of burst contributions for visualization.
    
    Returns per-period data including:
    - contribution: Burst score contribution
    - raw_value: Actual document count
    - baseline_value: Expected baseline
    - Method-specific metrics (MACD histogram, Kleinberg state, etc.)
    """
    start, end = _resolve_window(db, start, end)

    rows = burst_crud.get_points_in_range(db, ngram_id, method, start, end, limit=limit)
    
    # Convert tuples to Pydantic models
    # Tuple structure: (date, period_index, contribution, raw_value, baseline_value,
    #                   macd_short_ema, macd_long_ema, macd_line, macd_signal, macd_histogram,
    #                   kleinberg_state, state_probability, weight_contribution)
    points = [
        BurstPointDTO(
            date=r[0],
            period_index=r[1],
            contribution=r[2],
            raw_value=r[3],
            baseline_value=r[4],
            # MACD metrics
            macd_short_ema=r[5],
            macd_long_ema=r[6],
            macd_line=r[7],
            macd_signal=r[8],
            macd_histogram=r[9],
            # Kleinberg metrics
            kleinberg_state=r[10],
            state_probability=r[11],
            weight_contribution=r[12],
        )
        for r in rows
    ]
    
    return BurstPointsResponse(
        ngram_id=ngram_id,
        method=method,
        start=start,
        end=end,
        points=points
    )


@router.get("/bursts/leaderboard", response_model=BurstLeaderboardResponse)
def get_burst_leaderboard(
    method: BurstMethod = Query("macd", description="Burst detection method"),

    # Time window (affects score calculation and normalization)
    start: date | None = Query(None, description="Start date (inclusive)"),
    end: date | None = Query(None, description="End date (inclusive)"),

    # Normalization scope filters (applied BEFORE normalization)
    n_words: int | None = Query(None, description="Filter by n-gram length"),
    domain_id: int | None = Query(None, description="Filter by domain"),
    field_id: int | None = Query(None, description="Filter by field"),
    subfield_id: int | None = Query(None, description="Filter by subfield"),
    
    # Text search (applied AFTER normalization)
    ngram_text: str | None = Query(None, description="Exact n-gram text match"),

    # Pagination and sorting
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(100, ge=1, le=5000, description="Results per page"),
    sort_order: str = Query("desc", regex="^(asc|desc)$", description="Sort order"),

    db: Session = Depends(get_db),
):
    """
    Get ranked n-grams by burst score with normalization within filtered scope.
    
    **Normalization Behavior:**
    - Scores are normalized to 0-100 scale within the filtered subset
    - Normalization scope: (method + hierarchy + time + n_words)
    - This allows fair comparison within domains/fields/subfields
    
    **Filter Order:**
    1. Method filter
    2. Hierarchy filters (domain/field/subfield) → affects normalization
    3. Time range → affects normalization
    4. N-words filter → affects normalization
    5. Calculate scores
    6. Normalize within filtered scope
    7. Apply text search
    8. Paginate
    
    **Examples:**
    - `/bursts/leaderboard?method=macd&domain_id=1`
      → Normalized across Computer Science only
    - `/bursts/leaderboard?method=kleinberg&n_words=2&start=2020-01-01`
      → Normalized across 2-word phrases from 2020 onward
    """
    start, end = _resolve_window(db, start, end)

    rows, total = burst_crud.get_burst_leaderboard(
        db, method,
        start=start, end=end,
        n_words=n_words,
        subfield_id=subfield_id,
        field_id=field_id,
        domain_id=domain_id,
        ngram_text=ngram_text,
        page=page,
        page_size=page_size,
        sort_order=sort_order
    )

    # Convert dicts to Pydantic models (validation happens here)
    data = [BurstLeaderboardRow(**row) for row in rows]

    total_pages = (total + page_size - 1) // page_size
    pagination = PaginationMeta(
        page=page,
        page_size=page_size,
        total_count=total,
        total_pages=total_pages,
        has_next=page < total_pages,
        has_previous=page > 1
    )
    
    return BurstLeaderboardResponse(data=data, pagination=pagination)


@router.get("/bursts/time-bounds")
def get_time_bounds(db: Session = Depends(get_db)):
    """
    Get the min/max dates available in burst detection data.
    Useful for setting default time range filters in the UI.
    """
    min_date, max_date = db.query(
        func.min(BurstPoint.date),
        func.max(BurstPoint.date)
    ).one()
    
    return {
        "min": (min_date.isoformat() if min_date else None),
        "max": (max_date.isoformat() if max_date else None),
    }