# File: app/models/time_series.py
from sqlalchemy import Column, Integer, Date, Float, ForeignKey, UniqueConstraint, Index
from sqlalchemy.orm import relationship
from app.models.base import Base

class TimeSeries(Base):
    __tablename__ = "time_series"

    id = Column(Integer, primary_key=True, index=True)
    ngram_id = Column(Integer, ForeignKey("ngrams.id"), nullable=False)
    date = Column(Date, nullable=False)
    count = Column(Float, nullable=False)

    ngram = relationship(
        "Ngram", 
        backref="time_series")
    
    __table_args__ = (
        UniqueConstraint("ngram_id", "date", name="uq_ngram_date"),
        Index('idx_timeseries_ngram_date', 'ngram_id', 'date'),     # For time series queries
        Index("idx_timeseries_date", "date"),
    )