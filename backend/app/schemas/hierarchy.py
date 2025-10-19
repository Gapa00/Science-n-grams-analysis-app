# backend/app/schemas/hierarchy.py  
from pydantic import BaseModel
from typing import List

class SubfieldInfo(BaseModel):
    """Subfield information"""
    id: int
    name: str

class FieldInfo(BaseModel):
    """Field information with subfields"""
    id: int
    name: str
    subfields: List[SubfieldInfo]

class DomainInfo(BaseModel):
    """Domain information with fields and subfields"""
    id: int
    name: str
    fields: List[FieldInfo]

# No need for HierarchyResponse wrapper - just use List[DomainInfo] directly