# backend/app/api/api_v1/filters.py
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import Optional, List

from app.core.database import SessionLocal
from app.crud import hierarchy_crud
from app.schemas import DomainInfo, NgramAutocomplete
from app.models import Ngram

router = APIRouter()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/filters/hierarchy", response_model=List[DomainInfo])
def get_hierarchy(db: Session = Depends(get_db)):
    """Get complete domain/field/subfield hierarchy for filtering."""
    return hierarchy_crud.get_full_hierarchy(db)

@router.get("/filters/n_words", response_model=List[int])
def get_n_word_counts(db: Session = Depends(get_db)):
    """Get available n-word counts for filtering."""
    return hierarchy_crud.get_n_word_counts(db)

@router.get("/filters/ngram-text", response_model=List[NgramAutocomplete])
def autocomplete_ngram(
    q: str = Query(..., min_length=2, description="Search term (minimum 2 characters)"),
    subfield_id: Optional[int] = Query(None, description="Filter by subfield"),
    limit: int = Query(20, ge=1, le=100, description="Maximum results"),
    db: Session = Depends(get_db)
):
    """Fixed autocomplete with manual exact match priority."""
    
    try:
        q_norm = q.lower()  # Don't strip! Preserve spaces
        print(f"🔍 Raw search term: '{q}' → normalized: '{q_norm}'")
        
        if len(q_norm.strip()) < 2:  # Only check length after stripping
            return []
        
        final_results = []
        
        # Step 1: Explicitly look for EXACT match first
        exact_query = db.query(Ngram.text).filter(func.lower(Ngram.text) == q_norm)
        if subfield_id:
            exact_query = exact_query.filter(Ngram.subfield_id == subfield_id)
        
        exact_result = exact_query.first()
        if exact_result:
            final_results.append(exact_result[0])
            print(f"✅ Added exact match: '{exact_result[0]}'")
        else:
            print(f"❌ No exact match found for: '{q_norm}'")
        
        # Step 2: Get other matches with smart ordering
        # First get prefix matches (like "larger" for "large" OR "llm models" for "llm ")
        prefix_query = db.query(Ngram.text).filter(
            func.lower(Ngram.text).like(f"{q_norm}%"),
            func.lower(Ngram.text) != q_norm  # Exclude exact match
        )
        if subfield_id:
            prefix_query = prefix_query.filter(Ngram.subfield_id == subfield_id)
        
        # Debug: check total prefix matches
        prefix_count = prefix_query.count()
        print(f"🔍 Total prefix matches for '{q_norm}%': {prefix_count}")
        
        prefix_results = prefix_query.order_by(func.length(Ngram.text)).limit(15).all()
        prefix_texts = [row[0] for row in prefix_results]
        
        # Then get contains matches (like "analysis large" for "large")
        contains_query = db.query(Ngram.text).filter(
            func.lower(Ngram.text).like(f"%{q_norm}%"),
            func.lower(Ngram.text) != q_norm,  # Exclude exact match
            ~func.lower(Ngram.text).like(f"{q_norm}%")  # Exclude prefix matches
        )
        if subfield_id:
            contains_query = contains_query.filter(Ngram.subfield_id == subfield_id)
        
        # Debug: check total contains matches    
        contains_count = contains_query.count()
        print(f"🔍 Total contains matches for '%{q_norm}%': {contains_count}")
            
        contains_results = contains_query.order_by(Ngram.text).limit(15).all()
        contains_texts = [row[0] for row in contains_results]
        
        # Combine prefix + contains
        other_texts = prefix_texts + contains_texts
        
        # Debug info
        print(f"🔍 Prefix matches ({len(prefix_texts)}): {prefix_texts}")
        print(f"🔍 Contains matches ({len(contains_texts)}): {contains_texts[:10]}")
        
        # Deduplicate
        seen = {final_results[0]} if final_results else set()
        unique_others = []
        for text in other_texts:
            if text not in seen:
                unique_others.append(text)
                seen.add(text)
        
        print(f"🔍 Found {len(unique_others)} other matches: {unique_others[:10]}")
        
        # Step 3: Sort the "other" results by relevance
        def sort_key(text):
            text_lower = text.lower()
            # Starts with term (high priority)
            if text_lower.startswith(q_norm):
                return (0, len(text), text)
            # Word boundary matches
            elif (text_lower.startswith(f"{q_norm} ") or 
                  f" {q_norm} " in text_lower or 
                  text_lower.endswith(f" {q_norm}")):
                return (1, len(text), text)
            # Contains anywhere (low priority)
            else:
                return (2, len(text), text)
        
        sorted_others = sorted(unique_others, key=sort_key)
        
        # Step 4: Combine exact match + sorted others
        final_results.extend(sorted_others[:limit-1])  # Leave room for exact match
        final_results = final_results[:limit]
        
        print(f"🎯 Final results: {final_results}")
        
        return [NgramAutocomplete(id=None, text=text) for text in final_results]
        
    except Exception as e:
        print(f"💥 Error in autocomplete_ngram: {str(e)}")
        import traceback
        print(f"🔍 Full traceback: {traceback.format_exc()}")
        return []