# backend/app/api/api_v1/leaderboard.py
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from typing import Optional

from app.core.database import SessionLocal
from app.crud import ngram_crud
from app.schemas import NgramData, PaginatedNgramResponse, FrequencyResponse, PaginationMeta, FrequencyPoint

router = APIRouter()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/leaderboard", response_model=PaginatedNgramResponse)
def get_leaderboard(
    subfield_id: Optional[int] = None,
    field_id: Optional[int] = None,
    domain_id: Optional[int] = None,
    n_words: Optional[int] = None,
    ngram_text: Optional[str] = Query(None),
    ngram_id: Optional[int] = None,
    page: int = Query(1, ge=1, description="Page number (1-based)"),
    page_size: int = Query(1000, ge=1, le=5000, description="Items per page"),
    sort_by: str = Query("df_ngram_subfield", description="Sort field"),
    sort_order: str = Query("desc", description="Sort order: asc, desc"),
    db: Session = Depends(get_db)
):
    """Get paginated leaderboard of n-grams with complete metadata."""
    
    # Validate sort_by parameter
    valid_sort_fields = ["df_ngram_subfield", "df_ngram", "text", "n_words", "subfield", "field", "domain"]
    if sort_by not in valid_sort_fields:
        raise HTTPException(status_code=400, detail=f"Invalid sort_by field: {sort_by}")
    
    # Use CRUD layer for database operations
    results, total_count = ngram_crud.get_leaderboard(
        db=db,
        subfield_id=subfield_id,
        field_id=field_id,
        domain_id=domain_id,
        n_words=n_words,
        ngram_text = ngram_text,
        ngram_id=ngram_id,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_order=sort_order
    )
    
    # Calculate pagination metadata
    total_pages = (total_count + page_size - 1) // page_size
    
    # Format response using schemas
    ngrams_data = [
        NgramData(
            id=row.id,
            text=row.text,
            n_words=row.n_words,
            df_ngram=row.df_ngram,
            df_ngram_subfield=row.df_ngram_subfield,
            domain=row.domain,
            domain_id=row.domain_id,
            field=row.field,
            field_id=row.field_id,
            subfield=row.subfield,
            subfield_id=row.subfield_id
        )
        for row in results
    ]
    
    pagination_meta = PaginationMeta(
        page=page,
        page_size=page_size,
        total_count=total_count,
        total_pages=total_pages,
        has_next=page < total_pages,
        has_previous=page > 1
    )
    
    return PaginatedNgramResponse(
        data=ngrams_data,
        pagination=pagination_meta
    )

@router.get("/ngram/{ngram_id}", response_model=NgramData)
def get_ngram_details(ngram_id: int, db: Session = Depends(get_db)):
    """Get complete details for a specific n-gram."""
    
    result = ngram_crud.get_by_id(db, ngram_id)
    
    if not result:
        raise HTTPException(status_code=404, detail=f"N-gram with id {ngram_id} not found")
    
    return NgramData(
        id=result.id,
        text=result.text,
        n_words=result.n_words,
        df_ngram=result.df_ngram,
        df_ngram_subfield=result.df_ngram_subfield,
        domain=result.domain,
        domain_id=result.domain_id,
        field=result.field,
        field_id=result.field_id,
        subfield=result.subfield,
        subfield_id=result.subfield_id
    )

@router.get("/ngram/{ngram_id}/frequency", response_model=FrequencyResponse)
def get_frequency(ngram_id: int, db: Session = Depends(get_db)):
    """Get time series frequency data for a specific n-gram."""
    
    # Check if ngram exists and get details
    ngram_details = ngram_crud.get_by_id(db, ngram_id)
    if not ngram_details:
        raise HTTPException(status_code=404, detail=f"N-gram with id {ngram_id} not found")
    
    # Get frequency data
    frequency_data = ngram_crud.get_frequency_data(db, ngram_id)
    
    # Format frequency points
    frequency_points = [
        FrequencyPoint(
            date=row.date.isoformat(), 
            count=row.count
        ) 
        for row in frequency_data
    ]
    
    return FrequencyResponse(
        ngram_id=ngram_id,
        ngram_text=ngram_details.text,
        frequency_data=frequency_points
    )