# backend/app/schemas/ngram.py
from pydantic import BaseModel
from typing import List, Optional
from datetime import date

class NgramData(BaseModel):
    id: int
    text: str
    n_words: int
    df_ngram: float
    df_ngram_subfield: float
    domain: str
    domain_id: int
    field: str
    field_id: int
    subfield: str
    subfield_id: int

    class Config:
        from_attributes = True

class NgramAutocomplete(BaseModel):
    """Schema for autocomplete ngram suggestions"""
    id: Optional[int] = None
    text: str

    class Config:
        from_attributes = True

class PaginationMeta(BaseModel):
    page: int
    page_size: int
    total_count: int
    total_pages: int
    has_next: bool
    has_previous: bool

class PaginatedNgramResponse(BaseModel):
    data: List[NgramData]
    pagination: PaginationMeta

class FrequencyPoint(BaseModel):
    date: str  # ISO format date string
    count: float

class FrequencyResponse(BaseModel):
    "Time series Frequency response"
    ngram_id: int
    ngram_text: str
    frequency_data: List[FrequencyPoint]