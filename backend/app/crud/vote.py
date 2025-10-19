from sqlalchemy.orm import Session
from sqlalchemy import select
from app.models.binary_vote import BinaryVote
from app.models.slider_vote import SliderVote

# --- Binary Vote CRUD ---

def get_binary_vote_for_user_and_pair(db: Session, user_id: int, pair_index: int) -> BinaryVote | None:
    return db.scalar(
        select(BinaryVote).where(BinaryVote.user_id == user_id, BinaryVote.pair_index == pair_index)
    )

def create_or_update_binary_vote(
    db: Session,
    user_id: int,
    pair_index: int,
    left_id: int,
    right_id: int,
    choice: str,
    rt_ms: int | None,
):
    vote = get_binary_vote_for_user_and_pair(db, user_id, pair_index)
    if vote:
        vote.choice = choice
        vote.rt_ms = rt_ms
        vote.left_id = left_id
        vote.right_id = right_id
        db.add(vote)
    else:
        vote = BinaryVote(
            user_id=user_id,
            pair_index=pair_index,
            left_id=left_id,
            right_id=right_id,
            choice=choice,
            rt_ms=rt_ms,
        )
        db.add(vote)

    db.commit()
    return vote


# --- Slider Vote CRUD ---

def get_slider_vote_for_user_and_ngram(db: Session, user_id: int, ngram_id: int) -> SliderVote | None:
    return db.scalar(
        select(SliderVote).where(SliderVote.user_id == user_id, SliderVote.ngram_id == ngram_id)
    )

def create_or_update_slider_vote(
    db: Session,
    user_id: int,
    ngram_id: int,
    slider_value: float,
):
    vote = get_slider_vote_for_user_and_ngram(db, user_id, ngram_id)
    if vote:
        vote.slider_value = slider_value
        db.add(vote)
    else:
        vote = SliderVote(
            user_id=user_id,
            ngram_id=ngram_id,
            slider_value=slider_value,
        )
        db.add(vote)

    db.commit()
    return vote
