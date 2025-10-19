from sqlalchemy import Integer, String, DateTime, func, ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from app.models.base import Base

class BinaryVote(Base):
    __tablename__ = "binary_votes"
    __table_args__ = (UniqueConstraint("user_id", "pair_index", name="uq_binary_votes_user_pair"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    pair_index: Mapped[int] = mapped_column(Integer, nullable=False)
    choice: Mapped[str] = mapped_column(String(8), nullable=False)
    rt_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    left_id:  Mapped[int] = mapped_column(Integer, nullable=False)
    right_id: Mapped[int] = mapped_column(Integer, nullable=False)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    created_at: Mapped["DateTime"] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped["DateTime"] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
