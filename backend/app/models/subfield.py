# File: app/models/subfield.py
from sqlalchemy import Column, Integer, String, ForeignKey, UniqueConstraint, Index
from sqlalchemy.orm import relationship
from app.models.base import Base

class Subfield(Base):
    __tablename__ = "subfields"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    field_id = Column(Integer, ForeignKey("fields.id"), nullable=False)

    field = relationship("Field", backref="subfields")

    __table_args__ = (
        UniqueConstraint("field_id", "name", name="uq_field_subfield"),
        Index("idx_subfield_field", "field_id"),
    )
