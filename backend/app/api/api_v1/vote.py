from typing import Optional, Annotated, Literal
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.orm import Session
from sqlalchemy import select

from app.core.database import SessionLocal
from app.crud import user as user_crud
from app.crud import vote as vote_crud
from app.models import User, BinaryVote, SliderVote

# --- Import Schemas from dedicated files ---
from app.schemas.vote import SubmitVoteRequest, SubmitVoteResponse, SubmitSliderVoteRequest, SubmitSliderVoteResponse, NextPairResponse, GetPairResponse
from app.schemas.user import LoginRequest, LoginResponse

router = APIRouter()

# ---------- DB dependency ----------
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ---------- Helpers ----------
def _get_pairs_from_state(request: Request) -> list[dict]:
    """Helper function to fetch binary voting pairs from the app state."""
    pairs = getattr(request.app.state, "binary_vote_data", None)
    if not isinstance(pairs, list):
        return []
    return pairs

def _get_slider_data_from_state(request: Request) -> list[dict]:
    """Helper function to fetch slider voting ngram data from the app state."""
    slider_data = getattr(request.app.state, "slider_vote_data", None)
    if not isinstance(slider_data, list):
        return []
    return slider_data

# ---------- Endpoints ----------

# --- Binary Voting Endpoints ---
@router.post("/login", response_model=LoginResponse)
def login(payload: LoginRequest, db: Session = Depends(get_db)):
    u = user_crud.get_by_username(db, payload.username)
    if not u:
        u = user_crud.create(db, payload.username)
    return LoginResponse(user_id=u.id, username=u.username)

@router.get("/vote/binary/next", response_model=NextPairResponse)
def get_next_pair(
    request: Request,
    user_id: int = Query(..., gt=0),
    db: Session = Depends(get_db),
):
    pairs = _get_pairs_from_state(request)
    total = len(pairs)
    if total == 0:
        return NextPairResponse(done=True, total=0)

    # find first index, user hasn't voted on (1..total)
    voted_indices = {idx for (idx,) in db.execute(
        select(BinaryVote.pair_index).where(BinaryVote.user_id == user_id)
    ).all()}

    next_index = next((i for i in range(1, total + 1) if i not in voted_indices), None)
    if next_index is None:
        return NextPairResponse(done=True, total=total)

    pair = pairs[next_index - 1]
    
    # Ensure you're extracting just the IDs, not the entire object
    left_id = int(pair["left"]["id"])  # Extract 'id' from the 'left' object
    right_id = int(pair["right"]["id"])  # Extract 'id' from the 'right' object

    return NextPairResponse(
        done=False,
        total=total,
        index=next_index,
        left_id=left_id,
        right_id=right_id,
    )


@router.get("/vote/binary/pair", response_model=GetPairResponse)
def get_pair_by_index(
    request: Request,
    user_id: int = Query(..., gt=0),
    index: int = Query(..., gt=0),
    db: Session = Depends(get_db),
):
    """Fetch any pair by 1-based index and return the user's current choice if it exists."""
    pairs = _get_pairs_from_state(request)
    total = len(pairs)
    if index < 1 or index > total:
        raise HTTPException(status_code=404, detail="Pair index out of range")

    pair = pairs[index - 1]
    v = db.scalar(select(BinaryVote).where(BinaryVote.user_id == user_id, BinaryVote.pair_index == index))

    return GetPairResponse(
        total=total,
        index=index,
        left_id=int(pair["left"]["id"]),
        right_id=int(pair["right"]["id"]),
        choice=(v.choice if v else None),
    )

@router.post("/vote/binary/submit", response_model=SubmitVoteResponse)
def submit_binary_vote(payload: SubmitVoteRequest, db: Session = Depends(get_db)):
    """Submit binary vote for a pair."""
    # validate user existence
    u = db.get(User, payload.user_id)
    if not u:
        raise HTTPException(status_code=404, detail="User not found")

    vote_crud.create_or_update_binary_vote(
        db=db,
        user_id=payload.user_id,
        pair_index=payload.pair_index,
        left_id=payload.left_id,
        right_id=payload.right_id,
        choice=payload.choice,
        rt_ms=payload.rt_ms,
    )
    return SubmitVoteResponse(ok=True)


# --- Slider Voting Endpoints ---

@router.post("/vote/slider/submit", response_model=SubmitSliderVoteResponse)
def submit_slider_vote(payload: SubmitSliderVoteRequest, db: Session = Depends(get_db)):
    """Submit slider vote for an ngram."""
    # validate user existence
    u = db.get(User, payload.user_id)
    if not u:
        raise HTTPException(status_code=404, detail="User not found")

    vote_crud.create_or_update_slider_vote(
        db=db,
        user_id=payload.user_id,
        ngram_id=payload.ngram_id,
        slider_value=payload.slider_value,  # slider value from the request
    )
    return SubmitSliderVoteResponse(ok=True)

@router.get("/vote/slider/data", response_model=list[dict])
def get_slider_ngrams(request: Request):
    """Fetch all n-grams for slider voting."""
    slider_data = _get_slider_data_from_state(request)
    if not slider_data:
        raise HTTPException(status_code=404, detail="Slider vote data not found")
    return slider_data
