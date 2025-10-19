# app/schemas/bursts.py
"""
Pydantic schemas for burst detection API responses.
"""

from typing import Optional, List, Literal
from pydantic import BaseModel, Field
from datetime import date

from .ngram import PaginationMeta

# Type alias for burst detection methods
BurstMethod = Literal["macd", "kleinberg"]


class BurstPointDTO(BaseModel):
    """Single time point in a burst detection time series with FULL metrics."""
    date: date
    period_index: int
    contribution: float = Field(..., description="Burst contribution (can be negative for MACD)")
    raw_value: float = Field(..., description="Raw document/prevalence count")
    baseline_value: Optional[float] = Field(None, description="Expected baseline value")
    
    # ✅ NEW: Complete MACD metrics (null for Kleinberg)
    macd_short_ema: Optional[float] = Field(None, description="MACD short-period EMA")
    macd_long_ema: Optional[float] = Field(None, description="MACD long-period EMA (baseline)")
    macd_line: Optional[float] = Field(None, description="MACD line (short - long)")
    macd_signal: Optional[float] = Field(None, description="MACD signal line")
    macd_histogram: Optional[float] = Field(None, description="MACD histogram (line - signal)")
    
    # Kleinberg-specific fields (null for MACD)
    kleinberg_state: Optional[int] = Field(None, description="Kleinberg burst state (0=baseline, 1+=burst)")
    state_probability: Optional[float] = Field(None, description="Kleinberg state probability")
    weight_contribution: Optional[float] = Field(None, description="Kleinberg weight contribution")


class BurstScoreResponse(BaseModel):
    """Response for a single ngram's burst score in a time range."""
    ngram_id: int
    method: BurstMethod
    start: Optional[date] = None
    end: Optional[date] = None
    score: float = Field(..., description="Raw unnormalized burst score")


class BurstPointsResponse(BaseModel):
    """Time series data for a single ngram's burst detection."""
    ngram_id: int
    method: BurstMethod
    start: Optional[date] = Field(None, description="Start of time range (inclusive)")
    end: Optional[date] = Field(None, description="End of time range (inclusive)")
    points: List[BurstPointDTO] = Field(..., description="Time series of burst contributions")


class BurstLeaderboardRow(BaseModel):
    """Single row in the burst leaderboard."""
    ngram_id: int
    text: str = Field(..., description="N-gram text")
    n_words: int = Field(..., description="Number of words in n-gram")
    
    # Hierarchy metadata
    domain: str
    domain_id: int
    field: str
    field_id: int
    subfield: str
    subfield_id: int
    
    # Burst detection results
    method: BurstMethod
    score: float = Field(..., description="Raw unnormalized burst score")
    normalized_score: float = Field(..., description="Normalized score (0-100) within filtered scope")
    num_bursts: Optional[int] = Field(None, description="Number of distinct burst intervals")
    rank: Optional[int] = Field(None, description="Global rank (only in global mode)")


class BurstLeaderboardResponse(BaseModel):
    """Paginated burst leaderboard response."""
    data: List[BurstLeaderboardRow]
    pagination: PaginationMeta