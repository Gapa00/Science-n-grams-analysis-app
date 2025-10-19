from sqlalchemy import Integer, Float, DateTime, func, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column
from app.models.base import Base

class SliderVote(Base):
    __tablename__ = "slider_votes"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ngram_id: Mapped[int] = mapped_column(Integer, nullable=False)  # The unique ID for each ngram/graph
    slider_value: Mapped[float] = mapped_column(Float, nullable=False)  # Store slider value (0-100)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    
    created_at: Mapped["DateTime"] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped["DateTime"] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
