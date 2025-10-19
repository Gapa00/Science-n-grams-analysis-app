# File: app/models/ngram.py
from sqlalchemy import Column, Integer, String, Float, ForeignKey, UniqueConstraint, Index
from sqlalchemy.orm import relationship
from app.models.base import Base

class Ngram(Base):
    __tablename__ = "ngrams"

    id = Column(Integer, primary_key=True, index=True)
    text = Column(String, nullable=False)
    n_words = Column(Integer, nullable=False)
    subfield_id = Column(Integer, ForeignKey("subfields.id"), nullable=False)
    df_ngram = Column(Float, nullable=False)
    df_ngram_subfield = Column(Float, nullable=False)

    subfield = relationship("Subfield", backref="ngrams")

    burst_points = relationship("BurstPoint", back_populates="ngram", overlaps="burst_detection,points")

    __table_args__ = (
        UniqueConstraint("text", "subfield_id", name="uq_ngram_text_subfield"),
        Index('idx_ngram_text_subfield', 'text', 'subfield_id'),  # Fast lookup by text + subfield
    )