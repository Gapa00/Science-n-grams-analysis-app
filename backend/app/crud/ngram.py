# backend/app/crud/ngram.py
from sqlalchemy.orm import Session
from sqlalchemy import func, case, and_, or_
from typing import Optional, List, Tuple
from app.models import Domain, Field, Subfield, Ngram, TimeSeries

class NgramCRUD:
    """Database operations for N-grams"""
    
    def get_leaderboard(
        self,
        db: Session,
        subfield_id: Optional[int] = None,
        field_id: Optional[int] = None,
        domain_id: Optional[int] = None,
        n_words: Optional[int] = None,
        ngram_text: Optional[str] = None,
        ngram_id: Optional[int] = None,
        page: int = 1,
        page_size: int = 1000,
        sort_by: str = "text",
        sort_order: str = "desc"
    ) -> Tuple[List, int]:
        """
        Get paginated n-grams with full metadata and safe relevance scoring.
        Returns (results, total_count)
        """
        
        # Build base query with joins
        query_columns = [
            Ngram.id,
            Ngram.text,
            Ngram.n_words,
            Ngram.df_ngram,
            Ngram.df_ngram_subfield,
            Ngram.subfield_id,
            Subfield.id.label('subfield_id'),
            Subfield.name.label('subfield'),
            Field.id.label('field_id'),
            Field.name.label('field'),
            Domain.id.label('domain_id'),
            Domain.name.label('domain')
        ]
        
        # Add safe relevance score when searching
        if ngram_text:
            search_term = ngram_text.strip().lower()
            
            # Safe word boundary detection using LIKE patterns
            relevance_score = case(
                # Priority 1: Exact match
                (func.lower(Ngram.text) == search_term, 1),
                
                # Priority 2: Word boundary patterns (space-separated)
                (or_(
                    func.lower(Ngram.text).like(f"{search_term} %"),      # "llm something"
                    func.lower(Ngram.text).like(f"% {search_term} %"),    # "something llm else"
                    func.lower(Ngram.text).like(f"% {search_term}"),      # "something llm"
                ), 2),
                
                # Priority 3: Starts with term (prefix)
                (func.lower(Ngram.text).like(f"{search_term}%"), 3),
                
                # Priority 4: Contains anywhere (lowest)
                (func.lower(Ngram.text).like(f"%{search_term}%"), 4),
                
                else_=5
            ).label('relevance_score')
            
            # Length as secondary sort within same relevance
            length_score = func.length(Ngram.text).label('length_score')
            
            query_columns.extend([relevance_score, length_score])
        
        query = db.query(*query_columns).join(
            Subfield, Ngram.subfield_id == Subfield.id
        ).join(
            Field, Subfield.field_id == Field.id
        ).join(
            Domain, Field.domain_id == Domain.id
        )

        # Apply filters
        if subfield_id:
            query = query.filter(Ngram.subfield_id == subfield_id)
        elif field_id:
            query = query.filter(Subfield.field_id == field_id)
        elif domain_id:
            query = query.filter(Field.domain_id == domain_id)

        if n_words:
            query = query.filter(Ngram.n_words == n_words)
        
        if ngram_id:
            query = query.filter(Ngram.id == ngram_id)
            
        if ngram_text:
            # Safe search filter using simple LIKE patterns
            search_term = ngram_text.strip().lower()
            
            # Include all matches but rely on relevance scoring to prioritize
            search_conditions = or_(
                # Exact match
                func.lower(Ngram.text) == search_term,
                # Word boundaries
                func.lower(Ngram.text).like(f"{search_term} %"),
                func.lower(Ngram.text).like(f"% {search_term} %"),
                func.lower(Ngram.text).like(f"% {search_term}"),
                # Prefix
                func.lower(Ngram.text).like(f"{search_term}%"),
                # Contains (but this will have low relevance score)
                func.lower(Ngram.text).like(f"%{search_term}%")
            )
            
            query = query.filter(search_conditions)
        
        # Apply sorting with relevance priority
        if ngram_text:
            # When searching, relevance is ALWAYS first priority
            secondary_sort = self._get_sort_column(sort_by)
            
            # Order by: relevance (1=best), then length (shorter=better), then user choice
            if sort_order.lower() == "desc":
                query = query.order_by(
                    relevance_score.asc(),          # 1, 2, 3, 4 (lower = more relevant)
                    length_score.asc(),             # Shorter phrases better
                    secondary_sort.desc()           # User's sort choice
                )
            else:
                query = query.order_by(
                    relevance_score.asc(),          # 1, 2, 3, 4 (lower = more relevant)
                    length_score.asc(),             # Shorter phrases better
                    secondary_sort.asc()            # User's sort choice
                )
        else:
            # Normal sorting when not searching
            sort_column = self._get_sort_column(sort_by)
            if sort_order.lower() == "desc":
                query = query.order_by(sort_column.desc())
            else:
                query = query.order_by(sort_column.asc())
        
        # Get total count
        total_count = query.count()
        
        # Apply pagination
        offset = (page - 1) * page_size
        results = query.offset(offset).limit(page_size).all()
        
        return results, total_count
    
    def _get_sort_column(self, sort_by: str):
        """Get the appropriate column for sorting"""
        sort_column_map = {
            "df_ngram_subfield": Ngram.df_ngram_subfield,
            "df_ngram": Ngram.df_ngram,
            "text": Ngram.text,
            "n_words": Ngram.n_words, 
            "subfield": Subfield.name,
            "field": Field.name,
            "domain": Domain.name,
        }
        return sort_column_map.get(sort_by, Ngram.df_ngram_subfield)

    def get_by_id(self, db: Session, ngram_id: int):
        """Get single n-gram with full metadata"""
        return (
            db.query(
                Ngram.id,
                Ngram.text,
                Ngram.n_words,
                Ngram.df_ngram,
                Ngram.df_ngram_subfield,
                Ngram.subfield_id,
                Subfield.id.label('subfield_id'),
                Subfield.name.label('subfield'),
                Field.id.label('field_id'),
                Field.name.label('field'),
                Domain.id.label('domain_id'),
                Domain.name.label('domain')
            )
            .join(Subfield, Ngram.subfield_id == Subfield.id)
            .join(Field, Subfield.field_id == Field.id)
            .join(Domain, Field.domain_id == Domain.id)
            .filter(Ngram.id == ngram_id)
            .first()
        )
    
    def get_frequency_data(self, db: Session, ngram_id: int):
        """Get time series data for n-gram"""
        return (
            db.query(TimeSeries)
            .filter(TimeSeries.ngram_id == ngram_id)
            .order_by(TimeSeries.date)
            .all()
        )
    
    def exists(self, db: Session, ngram_id: int) -> bool:
        """Check if n-gram exists"""
        return db.query(Ngram).filter(Ngram.id == ngram_id).first() is not None

# Create instance
ngram_crud = NgramCRUD()