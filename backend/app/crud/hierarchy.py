# backend/app/crud/hierarchy.py
from sqlalchemy.orm import Session
from typing import List, Optional
from app.models import Domain, Field, Subfield, Ngram

class HierarchyCRUD:
    """Database operations for domain/field/subfield hierarchy"""
    
    def get_full_hierarchy(self, db: Session) -> List[dict]:
        """Get complete hierarchy with domains, fields, and subfields"""
        domains = db.query(Domain).all()
        hierarchy = []
        
        for domain in domains:
            domain_entry = {
                "id": domain.id,
                "name": domain.name,
                "fields": []
            }
            
            for field in domain.fields:
                field_entry = {
                    "id": field.id,
                    "name": field.name,
                    "subfields": []
                }
                
                for subfield in field.subfields:
                    field_entry["subfields"].append({
                        "id": subfield.id,
                        "name": subfield.name
                    })
                
                domain_entry["fields"].append(field_entry)
            
            hierarchy.append(domain_entry)
        
        return hierarchy
    
    def get_n_word_counts(self, db: Session) -> List[int]:
        """Get distinct n-word counts available"""
        return [
            row[0] for row in 
            db.query(Ngram.n_words).distinct().order_by(Ngram.n_words).all()
        ]
    
    def autocomplete_ngrams(
        self, 
        db: Session, 
        query_text: str, 
        subfield_id: Optional[int] = None,
        limit: int = 20
    ) -> List[dict]:
        """Autocomplete n-gram text search"""
        query = db.query(Ngram).filter(Ngram.text.ilike(f"%{query_text}%"))
        
        if subfield_id:
            query = query.filter(Ngram.subfield_id == subfield_id)
        
        results = query.limit(limit).all()
        
        return [{"id": n.id, "text": n.text} for n in results]

# Create instance
hierarchy_crud = HierarchyCRUD()