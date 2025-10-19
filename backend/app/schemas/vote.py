from pydantic import BaseModel, Field
from typing import Annotated, Optional
from decimal import Decimal
from typing import Literal

# --- Binary Vote Schemas ---
class SubmitVoteRequest(BaseModel):
    user_id: int
    pair_index: Annotated[int, Field(gt=0)]
    left_id: int
    right_id: int
    choice: Literal["left", "right"]
    rt_ms: Optional[Annotated[int, Field(ge=0)]] = None

class SubmitVoteResponse(BaseModel):
    ok: bool = True

# --- NextPairResponse Schema ---
class NextPairResponse(BaseModel):
    done: bool
    total: int
    index: Optional[int] = None   # 1-based pair index if not done
    left_id: Optional[int] = None
    right_id: Optional[int] = None

# --- GetPairResponse Schema ---
class GetPairResponse(BaseModel):
    total: int
    index: int
    left_id: int
    right_id: int
    choice: Optional[Literal["left", "right"]] = None  # present if user already voted on this index

# --- Slider Vote Schemas ---
class SubmitSliderVoteRequest(BaseModel):
    user_id: int
    ngram_id: int  # The ID of the ngram being evaluated
    slider_value: Decimal = Field(..., ge=0, le=100)  # Slider value (0-100)

class SubmitSliderVoteResponse(BaseModel):
    ok: bool = True
