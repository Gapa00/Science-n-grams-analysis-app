from sqlalchemy import Column, Integer, Float, ForeignKey, JSON, Index, String, Enum
from sqlalchemy.orm import relationship
from app.models.base import Base
import enum
from sqlalchemy.dialects.postgresql import ENUM as PGEnum

# ADD a single, shared instance (outside the class, reuse it everywhere):
BURSTMETHOD = PGEnum('kleinberg', 'macd', name='burstmethod', create_type=True)

class BurstDetection(Base):
    __tablename__ = "burst_detections"

    id = Column(Integer, primary_key=True, index=True)
    ngram_id = Column(Integer, ForeignKey("ngrams.id"), nullable=False)
    # Explicitly specify the enum type name and ensure it uses the correct values
    method = Column(BURSTMETHOD, nullable=False, index=True)

    # Core fields for both methods
    global_score = Column(Float, nullable=False)
    rank = Column(Integer, nullable=True, index=True)
    num_bursts = Column(Integer, nullable=False, default=0)
    burst_intervals = Column(JSON, nullable=True)  # [[start_iso, end_iso], ...] - for reference only

    # Method parameters (for reproducibility/reference)
    # Kleinberg parameters
    kleinberg_s_param = Column(Float, nullable=True)     # s parameter
    kleinberg_gamma_param = Column(Float, nullable=True) # gamma parameter

    # MACD parameters  
    macd_short_span = Column(Integer, nullable=True)     # e.g., 24
    macd_long_span = Column(Integer, nullable=True)      # e.g., 48
    macd_signal_span = Column(Integer, nullable=True)    # e.g., 12
    poisson_threshold = Column(Float, nullable=True)     # e.g., 2.0

    # Relationships
    ngram = relationship("Ngram", backref="burst_detections")
    points = relationship(
        "BurstPoint", 
        primaryjoin="and_(BurstDetection.ngram_id == BurstPoint.ngram_id, BurstDetection.method == BurstPoint.method)",
        foreign_keys="[BurstPoint.ngram_id, BurstPoint.method]",
        backref="burst_detection",
        cascade="all, delete-orphan",
        overlaps="ngram,burst_points"
    )
    __table_args__ = (
        # Ensure unique method per ngram
        Index("idx_burst_unique_method", "ngram_id", "method", unique=True),
        
        # Core indices
        Index("idx_burst_method", "method"),
        Index("idx_burst_score", "global_score"),
        Index("idx_burst_rank", "rank"),
        Index("idx_burst_ngram", "ngram_id"),
        Index("idx_burst_num_bursts", "num_bursts"),
        Index("idx_burst_method_score", "method", "global_score"),
        Index("idx_burst_method_rank", "method", "rank"),
    )