# app/models/burst_point.py
from datetime import date  # noqa: F401
from sqlalchemy import Column, Integer, Float, ForeignKey, Index, Date
from sqlalchemy.orm import relationship
from app.models.base import Base
from app.models.burst import BURSTMETHOD  # reuse the same PGEnum


class BurstPoint(Base):
    """
    Time series data for burst detection.
    
    For MACD: Full timeline with all MACD metrics at each time point
    For Kleinberg: Full timeline with burst weights (0 where no burst)
    """
    __tablename__ = "burst_points"

    id = Column(Integer, primary_key=True)

    # Joins / filters
    ngram_id = Column(
        Integer,
        ForeignKey("ngrams.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    method = Column(BURSTMETHOD, nullable=False, index=True)

    # Time axis & position within the series
    date = Column(Date, nullable=False, index=True)
    period_index = Column(Integer, nullable=False)

    # Common per-period metrics
    contribution = Column(Float, nullable=False, default=0.0)  # Can be negative for MACD
    raw_value = Column(Float, nullable=False, default=0.0)
    baseline_value = Column(Float, nullable=True)

    # MACD-only metrics (nullable for Kleinberg)
    macd_short_ema = Column(Float, nullable=True, comment="MACD short-period EMA")
    macd_long_ema = Column(Float, nullable=True, comment="MACD long-period EMA (baseline)")
    macd_line = Column(Float, nullable=True, comment="MACD line (short_ema - long_ema)")
    macd_signal = Column(Float, nullable=True, comment="MACD signal line")
    macd_histogram = Column(Float, nullable=True, comment="MACD histogram (macd_line - signal)")

    # Kleinberg-only (nullable for MACD)
    kleinberg_state = Column(Integer, nullable=True, comment="Burst state (0=baseline, 1+=burst)")
    state_probability = Column(Float, nullable=True, comment="State transition probability")
    weight_contribution = Column(Float, nullable=True, comment="Kleinberg burst weight")

    # ORM relationship
    ngram = relationship(
        "Ngram",
        back_populates="burst_points",
        overlaps="burst_detection,points",
    )

    __table_args__ = (
        # COVERING index for range/interval queries
        Index("ix_bp_interval_query", "method", "date", "ngram_id", "contribution"),
        # Fast scans of a single ngram over time
        Index("ix_bp_ngram_method_date", "ngram_id", "method", "date"),
        # Kleinberg state filters
        Index("ix_bp_method_kleinberg_state", "method", "kleinberg_state"),
    )